package io.digdag.standards.operator.td;

import java.util.List;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.io.InputStream;
import java.io.IOException;
import com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static io.digdag.standards.operator.td.TDOperator.defaultRetryExecutor;

public class TDJobOperator
{
    private final TDClient client;
    private final String jobId;
    private TDJobSummary lastStatus;

    private static final int maxInterval = 30000;

    TDJobOperator(TDClient client, String jobId)
    {
        this.client = client;
        this.jobId = jobId;
    }

    public TDJobSummary joinJob()
    {
        try {
            return ensureSucceeded();
        }
        catch (TDJobException ex) {
            try {
                TDJob job = getJobInfo();
                String message = job.getCmdOut() + "\n" + job.getStdErr();
                throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
            }
            catch (Exception getJobInfoFailed) {
                getJobInfoFailed.addSuppressed(ex);
                throw Throwables.propagate(getJobInfoFailed);
            }
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        finally {
            ensureFinishedOrKill();
        }
    }

    public String getJobId()
    {
        return jobId;
    }

    public synchronized TDJobSummary join()
        throws InterruptedException
    {
        long start = System.currentTimeMillis();
        int interval = 1000;
        if (lastStatus == null) {
            updateLastStatus();
        }
        while (!lastStatus.getStatus().isFinished()) {
            Thread.sleep(interval);
            interval = Math.min(interval * 2, maxInterval);
            updateLastStatus();
        }
        return lastStatus;
    }

    private void updateLastStatus()
    {
        try {
            this.lastStatus = defaultRetryExecutor
                .run(() -> client.jobStatus(jobId));
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

    public TDJob getJobInfo()
    {
        try {
            return defaultRetryExecutor
                .run(() -> client.jobInfo(jobId));
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

    public TDJobSummary ensureSucceeded()
        throws TDJobException, InterruptedException
    {
        TDJobSummary summary = join();
        if (summary.getStatus() != TDJob.Status.SUCCESS) {
            throw new TDJobException("TD job " + jobId + " failed with status " + summary.getStatus(), jobId, summary);
        }
        return summary;
    }

    public TDJobSummary ensureRunningOrSucceeded()
            throws TDJobException, InterruptedException
    {
        if (lastStatus == null) {
            updateLastStatus();
        }
        TDJob.Status status = lastStatus.getStatus();
        if (!status.isFinished()) {
            return lastStatus;
        }
        if (status != TDJob.Status.SUCCESS) {
            throw new TDJobException("TD job " + jobId + " failed with status " + status, jobId, lastStatus);
        }
        return lastStatus;
    }

    public synchronized void ensureFinishedOrKill()
    {
        if (lastStatus == null || !lastStatus.getStatus().isFinished()) {
            try {
                defaultRetryExecutor.run(() -> client.killJob(jobId));
            }
            catch (RetryGiveupException ex) {
                throw Throwables.propagate(ex.getCause());
            }
        }
    }

    public List<String> getResultColumnNames()
    {
        return getJobInfo().getResultSchema().transform(js -> {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            try {
                ArrayNode array = (ArrayNode) new ObjectMapper().readTree(js);
                Iterator<JsonNode> elements = array.elements();
                while (elements.hasNext()) {
                    ArrayNode pair = (ArrayNode) elements.next();
                    builder.add(pair.get(0).textValue());
                }
                return builder.build();
            }
            catch (IOException | RuntimeException ex) {
                throw new RuntimeException("Unexpected hive_result_schema: " + js, ex);
            }
        })
        .or(ImmutableList.of());
    }

    public <R> R getResult(Function<Iterator<ArrayValue>, R> resultStreamHandler)
    {
        try {
            return defaultRetryExecutor.run(() ->
                    client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, (in) -> {
                        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(in, 32*1024))) {
                            return resultStreamHandler.apply(new Iterator<ArrayValue>() {
                                public boolean hasNext()
                                {
                                    try {
                                        return unpacker.hasNext();
                                    }
                                    catch (IOException ex) {
                                        throw Throwables.propagate(ex);
                                    }
                                }

                                public ArrayValue next()
                                {
                                    try {
                                        return unpacker.unpackValue().asArrayValue();
                                    }
                                    catch (IOException ex) {
                                        throw Throwables.propagate(ex);
                                    }
                                }
                            });
                        }
                        catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                    })
            );
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }
}

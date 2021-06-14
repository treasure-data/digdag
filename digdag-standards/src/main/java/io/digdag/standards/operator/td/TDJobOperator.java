package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

class TDJobOperator extends BaseTDOperator
{
    private final String jobId;

    TDJobOperator(TDClient client, String jobId, SecretProvider secrets)
    {
        this.client = client;
        this.jobId = jobId;
        this.secrets = secrets;
    }

    String getJobId()
    {
        return jobId;
    }

    TDJobSummary getJobStatus()
    {
        return callWithRetry(() -> client.jobStatus(jobId));
    }

    TDJob getJobInfo()
    {
        return callWithRetry(() -> client.jobInfo(jobId));
    }

    TDJobSummary ensureRunningOrSucceeded()
            throws TDJobException, InterruptedException
    {
        TDJobSummary summary = getJobStatus();
        TDJob.Status status = summary.getStatus();
        if (!status.isFinished()) {
            return summary;
        }
        if (status != TDJob.Status.SUCCESS) {
            throw new TDJobException("TD job " + jobId + " failed with status " + status, jobId, summary);
        }
        return summary;
    }

    List<String> getResultColumnNames()
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

    <R> R getResult(Function<Iterator<ArrayValue>, R> resultStreamHandler)
    {
        return callWithRetry(() ->
            client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, (in) -> {
                try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(in, 32 * 1024))) {
                    return resultStreamHandler.apply(new Iterator<ArrayValue>()
                    {
                        public boolean hasNext()
                        {
                            try {
                                return unpacker.hasNext();
                            }
                            catch (IOException ex) {
                                throw new UncheckedIOException(ex);
                            }
                        }

                        public ArrayValue next()
                        {
                            try {
                                return unpacker.unpackValue().asArrayValue();
                            }
                            catch (IOException ex) {
                                throw new UncheckedIOException(ex);
                            }
                        }
                    });
                }
                catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }));
    }

    TDJobSummary checkStatus()
    {
        try {
            return ensureRunningOrSucceeded();
        }
        catch (TDJobException ex) {
            try {
                TDJob job = getJobInfo();
                String message = job.getCmdOut() + "\n" + job.getStdErr();
                throw new TaskExecutionException(message, ex);
            }
            catch (Exception getJobInfoFailed) {
                getJobInfoFailed.addSuppressed(ex);
                throw ThrowablesUtil.propagate(getJobInfoFailed);
            }
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ThrowablesUtil.propagate(ex);
        }
    }
}

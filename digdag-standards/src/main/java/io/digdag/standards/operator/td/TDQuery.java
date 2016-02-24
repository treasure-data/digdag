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
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

public class TDQuery
{
    private final TDClient client;
    private final TDJobRequest request;
    private final String jobId;
    private TDJobSummary lastStatus;

    private final static int maxInterval = 30000;

    public TDQuery(TDClient client, TDJobRequest request)
    {
        this.client = client;
        this.request = request;
        this.jobId = client.submit(request);
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
            lastStatus = client.jobStatus(jobId);
        }
        while (!lastStatus.getStatus().isFinished()) {
            Thread.sleep(interval);
            interval = Math.min(interval * 2, maxInterval);
            lastStatus = client.jobStatus(jobId);
        }
        return lastStatus;
    }

    public TDJob getJobInfo()
    {
        return client.jobInfo(jobId);
    }

    public void ensureSucceeded()
        throws InterruptedException
    {
        TDJobSummary status = join();
        if (status.getStatus() != TDJob.Status.SUCCESS) {
            throw new RuntimeException("TD job " + jobId + " failed with status " + status.getStatus());
        }
    }

    public synchronized void ensureFinishedOrKill()
    {
        if (lastStatus == null || !lastStatus.getStatus().isFinished()) {
            client.killJob(jobId);
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

    public <R> R getResult(Function<Iterator<Value>, R> resultStreamHandler)
    {
        return client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, (in) -> {
            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(in, 32*1024))) {
                return resultStreamHandler.apply(new Iterator<Value>() {
                    public boolean hasNext()
                    {
                        try {
                            return unpacker.hasNext();
                        }
                        catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                    }

                    public Value next()
                    {
                        try {
                            return unpacker.unpackValue();
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
        });
    }
}

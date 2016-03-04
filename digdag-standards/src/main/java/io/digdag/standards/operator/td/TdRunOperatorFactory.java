package io.digdag.standards.operator.td;

import java.util.Date;
import java.util.List;
import java.time.Instant;
import java.nio.file.Path;
import java.io.File;
import java.io.BufferedWriter;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.operator.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;
import org.msgpack.value.ArrayValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.standards.operator.td.TdOperatorFactory.joinJob;

public class TdRunOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdRunOperatorFactory.class);

    @Inject
    public TdRunOperatorFactory()
    { }

    public String getType()
    {
        return "td_run";
    }

    @Override
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new TdRunOperator(archivePath, request);
    }

    private class TdRunOperator
            extends BaseOperator
    {
        public TdRunOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String name = params.get("_command", String.class);
            Instant sessionTime = params.get("session_time", Instant.class);
            Optional<String> downloadFile = params.getOptional("download_file", String.class);

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobOperator j = op.startSavedQuery(name, Date.from(sessionTime));
                logger.info("Started a saved query name={} with time={}", name, sessionTime);

                TDJobSummary summary = joinJob(j, archive, downloadFile);

                Config storeParams = request.getConfig().getFactory().create()
                    .set("td", request.getConfig().getFactory().create()
                            .set("last_job_id", summary.getJobId()));

                return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
            }
            return null;
        }
    }
}

package io.digdag.standards.operator.td;

import java.util.Date;
import java.time.Instant;
import java.nio.file.Path;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import com.treasuredata.client.model.TDJobSummary;

import static io.digdag.standards.operator.td.TdOperatorFactory.downloadJobResult;
import static io.digdag.standards.operator.td.TdOperatorFactory.buildStoreParams;

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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdRunOperator(workspacePath, request);
    }

    private class TdRunOperator
            extends BaseOperator
    {
        public TdRunOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String name = params.get("_command", String.class);
            Instant sessionTime = params.get("session_time", Instant.class);
            Optional<String> downloadFile = params.getOptional("download_file", String.class);
            boolean storeLastResults = params.get("store_last_results", boolean.class, false);
            boolean preview = params.get("preview", boolean.class, false);

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobOperator j = op.startSavedQuery(name, Date.from(sessionTime));
                logger.info("Started a saved query name={} with time={}", name, sessionTime);

                TDJobSummary summary = j.joinJob();
                downloadJobResult(j, workspace, downloadFile);

                if (preview) {
                    try {
                        TdOperatorFactory.downloadPreviewRows(j, "job id " + j.getJobId());
                    }
                    catch (Exception ex) {
                        logger.info("Getting rows for preview failed. Ignoring this error.", ex);
                    }
                }

                Config storeParams = buildStoreParams(request.getConfig().getFactory(), j, summary, storeLastResults);

                return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
            }
        }
    }
}

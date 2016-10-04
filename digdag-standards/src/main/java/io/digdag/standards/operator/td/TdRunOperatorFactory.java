package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static io.digdag.standards.operator.td.TdOperatorFactory.buildStoreParams;
import static io.digdag.standards.operator.td.TdOperatorFactory.downloadJobResult;

public class TdRunOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdRunOperatorFactory.class);
    private final Map<String, String> env;

    @Inject
    public TdRunOperatorFactory(@Environment Map<String, String> env)
    {
        this.env = env;
    }

    public String getType()
    {
        return "td_run";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new TdRunOperator(projectPath, request);
    }

    private class TdRunOperator
            extends BaseTdJobOperator
    {
        private final Config params;
        private final String name;
        private final Instant sessionTime;
        private final Optional<String> downloadFile;
        private final boolean storeLastResults;
        private final boolean preview;

        private TdRunOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, env);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.name = params.get("_command", String.class);
            this.sessionTime = params.get("session_time", Instant.class);
            this.downloadFile = params.getOptional("download_file", String.class);
            this.storeLastResults = params.get("store_last_results", boolean.class, false);
            this.preview = params.get("preview", boolean.class, false);
        }

        @Override
        protected String startJob(TaskExecutionContext ctx, TDOperator op, String domainKey)
        {
            TDSavedQueryStartRequest req = TDSavedQueryStartRequest.builder()
                    .name(name)
                    .scheduledTime(Date.from(sessionTime))
                    .domainKey(domainKey)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.startSavedQuery(req));
            logger.info("Started a saved query name={} with time={}, job id= {}", name, sessionTime, jobId);
            return jobId;
        }

        @Override
        protected TaskResult processJobResult(TaskExecutionContext ctx, TDOperator op, TDJobOperator job)
        {
            downloadJobResult(job, workspace, downloadFile, state);

            if (preview) {
                TdOperatorFactory.downloadPreviewRows(job, "job id " + job.getJobId());
            }

            Config storeParams = buildStoreParams(request.getConfig().getFactory(), job, storeLastResults);

            return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
        }
    }
}

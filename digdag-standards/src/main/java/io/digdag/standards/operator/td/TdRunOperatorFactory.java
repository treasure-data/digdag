package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import static io.digdag.standards.operator.td.TdOperatorFactory.buildStoreParams;
import static io.digdag.standards.operator.td.TdOperatorFactory.downloadJobResult;

public class TdRunOperatorFactory
        implements OperatorFactory
{
    private static final String JOB_ID = "jobId";
    private static final String DOMAIN_KEY = "domainKey";
    private static final String POLL_ITERATION = "pollIteration";

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30000;

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
        private final Config params;
        private final String name;
        private final Instant sessionTime;
        private final Optional<String> downloadFile;
        private final boolean storeLastResults;
        private final boolean preview;
        private final Config state;
        private final Optional<String> existingJobId;
        private final Optional<String> existingDomainKey;

        public TdRunOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.name = params.get("_command", String.class);
            this.sessionTime = params.get("session_time", Instant.class);
            this.downloadFile = params.getOptional("download_file", String.class);
            this.storeLastResults = params.get("store_last_results", boolean.class, false);
            this.preview = params.get("preview", boolean.class, false);

            this.state = request.getLastStateParams().deepCopy();

            this.existingJobId = state.getOptional(JOB_ID, String.class);
            this.existingDomainKey = state.getOptional(DOMAIN_KEY, String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Generate and store domain key before starting the job
                if (!existingDomainKey.isPresent()) {
                    String domainKey = UUID.randomUUID().toString();
                    state.set(DOMAIN_KEY, domainKey);
                    throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
                }

                // Start the job
                if (!existingJobId.isPresent()) {
                    TDJobOperator j = op.startSavedQuery(name, Date.from(sessionTime), existingDomainKey.get());
                    logger.info("Started a saved query name={} with time={}", name, sessionTime);
                    state.set(JOB_ID, j.getJobId());
                    state.set(POLL_ITERATION, 1);
                    throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Check if the job is done
                String jobId = existingJobId.get();
                TDJobOperator job = op.newJobOperator(jobId);
                TDJobSummary status = job.checkStatus();
                boolean done = status.getStatus().isFinished();
                if (!done) {
                    int pollIteration = state.get(POLL_ITERATION, int.class, 1);
                    state.set(POLL_ITERATION, pollIteration + 1);
                    int pollInterval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, pollIteration), MAX_POLL_INTERVAL);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                // Process result
                downloadJobResult(job, workspace, downloadFile);
                if (preview) {
                    try {
                        TdOperatorFactory.downloadPreviewRows(job, "job id " + job.getJobId());
                    }
                    catch (Exception ex) {
                        logger.info("Getting rows for preview failed. Ignoring this error.", ex);
                    }
                }

                Config storeParams = buildStoreParams(request.getConfig().getFactory(), job, status, storeLastResults);

                return TaskResult.defaultBuilder(request)
                        .storeParams(storeParams)
                        .build();
            }
        }
    }
}

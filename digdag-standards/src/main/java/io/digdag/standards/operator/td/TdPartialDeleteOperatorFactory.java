package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
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

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public class TdPartialDeleteOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdPartialDeleteOperatorFactory.class);

    private static final String JOB_ID = "jobId";
    private static final String POLL_ITERATION = "pollIteration";

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30000;

    @Inject
    public TdPartialDeleteOperatorFactory()
    { }

    public String getType()
    {
        return "td_partial_delete";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdPartialDeleteOperator(workspacePath, request);
    }

    private class TdPartialDeleteOperator
            extends BaseOperator
    {
        private final Config params;
        private final String table;
        private final Instant from;
        private final Instant to;
        private final Config state;
        private final Optional<String> existingJobId;

        public TdPartialDeleteOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.table = params.get("_command", String.class);
            this.from = params.get("from", TimestampParam.class).getTimestamp().toInstant(request.getTimeZone());
            this.to = params.get("to", TimestampParam.class).getTimestamp().toInstant(request.getTimeZone());

            if (from.getEpochSecond() % 3600 != 0 || to.getEpochSecond() % 3600 != 0) {
                throw new ConfigException("'from' and 'to' parameters must be whole hours");
            }

            this.state = request.getLastStateParams().deepCopy();

            this.existingJobId = state.getOptional(JOB_ID, String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Start the job
                if (!existingJobId.isPresent()) {
                    String jobId = startJob(op);
                    state.set(JOB_ID, jobId);
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

                return TaskResult.empty(request);
            }
        }

        private String startJob(TDOperator op)
        {
            TDJobOperator j = op.submitPartialDeleteJob(table, from, to);
            logger.info("Started partial delete job id={}", j.getJobId());
            return j.getJobId();
        }
    }
}

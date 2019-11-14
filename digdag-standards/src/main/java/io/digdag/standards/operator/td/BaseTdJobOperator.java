package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.treasuredata.client.TDClientException;
import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.td.TDOperator.SystemDefaultConfig;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

abstract class BaseTdJobOperator
        extends BaseOperator
{
    private static final String DONE_JOB_ID = "doneJobId";

    protected final TaskState state;
    protected final Config params;
    private final Map<String, String> env;

    protected final DurationInterval pollInterval;
    protected final DurationInterval retryInterval;
    protected final SystemDefaultConfig systemDefaultConfig;

    protected final BaseTDClientFactory clientFactory;

    private static Logger logger = LoggerFactory.getLogger(BaseTdJobOperator.class);

    BaseTdJobOperator(OperatorContext context, Map<String, String> env, Config systemConfig, BaseTDClientFactory clientFactory)
    {
        super(context);

        this.params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("td"));

        this.state = TaskState.of(request);
        this.env = env;

        this.pollInterval = TDOperator.pollInterval(systemConfig);
        this.retryInterval = TDOperator.retryInterval(systemConfig);
        this.systemDefaultConfig = TDOperator.systemDefaultConfig(systemConfig);
        this.clientFactory = clientFactory;
    }

    @Override
    public final TaskResult runTask()
    {
        try (TDOperator op = TDOperator.fromConfig(clientFactory, systemDefaultConfig, env, params, context.getSecrets().getSecrets("td"))) {
            Optional<String> doneJobId = state.params().getOptional(DONE_JOB_ID, String.class);
            TDJobOperator job;
            if (!doneJobId.isPresent()) {
                job = op.runJob(state, "job", pollInterval, retryInterval, (jobOperator, domainKey) -> startJob(jobOperator, domainKey));
                state.params().set(DONE_JOB_ID, job.getJobId());
            }
            else {
                job = op.newJobOperator(doneJobId.get());
            }

            // Get the job results
            TaskResult taskResult = processJobResult(op, job);

            long numRecords = 0L;
            try {
                // job.getJobInfo() may throw error after having retried 3 times
                numRecords = job.getJobInfo().getNumRecords();
            }
            catch (Exception ex) {
                logger.warn("Setting num_records failed. Ignoring this error.", ex);
            }

            // Set last_job_id param
            taskResult.getStoreParams()
                    .getNestedOrSetEmpty("td")
                    .set("last_job_id", job.getJobId()) // for compatibility with old style
                    .getNestedOrSetEmpty("last_job")
                    .set("id", job.getJobId())
                    .set("num_records", numRecords);

            return taskResult;
        }
        catch (TDClientException ex) {
            throw propagateTDClientException(ex);
        }
    }

    protected static Optional<String> poolNameOfEngine(Config params, String engine)
    {
        return params.getOptional(engine + "_pool_name", String.class);
    }

    protected static TaskExecutionException propagateTDClientException(TDClientException ex)
    {
        return new TaskExecutionException(ex);
    }

    protected abstract String startJob(TDOperator op, String domainKey);

    protected TaskResult processJobResult(TDOperator op, TDJobOperator job)
    {
        return TaskResult.empty(request);
    }
}

package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

abstract class BaseTdJobOperator
        extends BaseOperator
{
    private static final String DONE_JOB_ID = "doneJobId";

    protected final Config state;
    protected final Config params;
    private final Map<String, String> env;

    protected final DurationInterval pollInterval;
    protected final DurationInterval retryInterval;

    BaseTdJobOperator(Path workspacePath, TaskRequest request, Map<String, String> env, Config systemConfig)
    {
        super(workspacePath, request);

        this.params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("td"));

        this.state = request.getLastStateParams().deepCopy();
        this.env = env;

        this.pollInterval = TDOperator.pollInterval(systemConfig);
        this.retryInterval = TDOperator.retryInterval(systemConfig);
    }

    @Override
    public List<String> secretSelectors()
    {
        return ImmutableList.of("td.*");
    }

    @Override
    public final TaskResult runTask(TaskExecutionContext ctx)
    {
        try (TDOperator op = TDOperator.fromConfig(env, params, ctx.secrets().getSecrets("td"))) {

            Optional<String> doneJobId = state.getOptional(DONE_JOB_ID, String.class);
            TDJobOperator job;
            if (!doneJobId.isPresent()) {
                job = op.runJob(state, "job", pollInterval, retryInterval, (jobOperator, domainKey) -> startJob(ctx, jobOperator, domainKey));
                state.set(DONE_JOB_ID, job.getJobId());
            }
            else {
                job = op.newJobOperator(doneJobId.get());
            }

            // Get the job results
            TaskResult taskResult = processJobResult(ctx, op, job);

            // Set last_job_id param
            taskResult.getStoreParams()
                    .getNestedOrSetEmpty("td")
                    .set("last_job_id", job.getJobId());

            return taskResult;
        }
    }

    protected abstract String startJob(TaskExecutionContext ctx, TDOperator op, String domainKey);

    protected TaskResult processJobResult(TaskExecutionContext ctx, TDOperator op, TDJobOperator job)
    {
        return TaskResult.empty(request);
    }
}

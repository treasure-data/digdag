package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;

abstract class BaseTdJobOperator
        extends BaseOperator
{
    private static final String DONE_JOB_ID = "doneJobId";

    protected final Config state;
    protected final Config params;

    BaseTdJobOperator(Path workspacePath, TaskRequest request)
    {
        super(workspacePath, request);

        this.params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("td"));

        this.state = request.getLastStateParams().deepCopy();
    }

    @Override
    public final TaskResult runTask()
    {
        try (TDOperator op = TDOperator.fromConfig(params)) {

            Optional<String> doneJobId = state.getOptional(DONE_JOB_ID, String.class);
            TDJobOperator job;
            if (!doneJobId.isPresent()) {
                job = op.runJob(state, "job", this::startJob);
                state.set(DONE_JOB_ID, job.getJobId());
            }
            else {
                job = op.newJobOperator(doneJobId.get());
            }

            // Get the job results
            TaskResult taskResult = processJobResult(op, job);

            // Set last_job_id param
            Config storeParams = taskResult.getStoreParams()
                    .set("td", request.getConfig().getFactory().create()
                            .set("last_job_id", job.getJobId()));

            return TaskResult.builder().from(taskResult)
                    .storeParams(storeParams)
                    .build();
        }
    }

    protected abstract String startJob(TDOperator op, String domainKey);

    protected TaskResult processJobResult(TDOperator op, TDJobOperator job)
    {
        return TaskResult.empty(request);
    }
}

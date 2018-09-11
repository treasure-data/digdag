package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.model.TDJob;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

import java.util.Map;

public class TdJobShowOperatorFactory
        implements OperatorFactory
{

    private final Map<String, String> env;
    private final Config systemConfig;

    @Inject
    public TdJobShowOperatorFactory(@Environment Map<String, String> env, Config systemConfig)
    {
        this.env = env;
        this.systemConfig = systemConfig;
    }

    @Override
    public String getType()
    {
        return "td_job_show";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new TdJobShowOperator(context, env, systemConfig);
    }

    private class TdJobShowOperator
            extends BaseOperator
    {
        private final String jobId;
        private final Config params;
        private final Map<String, String> env;
        private final TDOperator.SystemDefaultConfig systemDefaultConfig;
        private final Optional<String> storeKey;

        public TdJobShowOperator(OperatorContext context, Map<String, String> env, Config systemConfig)
        {
            super(context);
            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.env = env;
            this.systemDefaultConfig = TDOperator.systemDefaultConfig(systemConfig);
            this.jobId = params.get("_command", String.class);
            this.storeKey = params.getOptional("store_key", String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(systemDefaultConfig, env, params, context.getSecrets().getSecrets("td"))) {
                TDJobOperator job = op.newJobOperator(jobId);
                TDJob jobInfo = job.getJobInfo();

                TaskResult taskResult = TaskResult.empty(request);
                Config storeParams;
                if (this.storeKey.isPresent()) {
                    storeParams = taskResult
                            .getStoreParams()
                            .getNestedOrSetEmpty(storeKey.get())
                            .getNestedOrSetEmpty(jobInfo.getJobId());
                }
                else {
                    storeParams = taskResult.getStoreParams()
                            .getNestedOrSetEmpty("td")
                            .getNestedOrSetEmpty("stored_jobs")
                            .getNestedOrSetEmpty(jobInfo.getJobId());
                }

                storeParams
                        .set("id", jobInfo.getJobId())
                        .set("num_records", jobInfo.getNumRecords())
                        .set("status", jobInfo.getStatus().toString())
                        .set("type", jobInfo.getType().toString())
                        .set("query", jobInfo.getQuery())
                        .set("result_schema", jobInfo.getResultSchema().or(""))
                        .set("created_at", jobInfo.getCreatedAt())
                        .set("start_at", jobInfo.getStartAt())
                        .set("end_at", jobInfo.getEndAt());

                return taskResult;
            }
            catch (
                    TDClientException ex) {
                throw new TaskExecutionException(ex);
            }
        }
    }
}

package io.digdag.standards.operator.td;

import com.google.inject.Inject;
import com.treasuredata.client.model.TDExportResultJobRequest;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.UserSecretTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TDResultExportOperatorFactory
        implements OperatorFactory
{
    private final Map<String, String> env;
    private final Config systemConfig;
    private static Logger logger = LoggerFactory.getLogger(TdTableExportOperatorFactory.class);

    @Inject
    public TDResultExportOperatorFactory(@Environment Map<String, String> env, Config systemConfig)
    {
        this.env = env;
        this.systemConfig = systemConfig;
    }

    @Override
    public String getType() { return "td_result_export"; }

    @Override
    public Operator newOperator(OperatorContext context) { return new TDResultExportOperator(context); }

    private class TDResultExportOperator
            extends BaseTdJobOperator
    {
        private final String jobId;
        private final String resultUrl;

        private TDResultExportOperator(OperatorContext context)
        {
            super(context, env, systemConfig);
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.jobId = params.get("job_id", String.class);
            this.resultUrl = UserSecretTemplate.of(params.get("result_url", String.class))
                    .format(context.getSecrets());
        }

        @Override
        protected String startJob(TDOperator op, String domainKey)
        {
            TDExportResultJobRequest jobRequest = TDExportResultJobRequest.builder()
                    .jobId(this.jobId)
                    .resultOutput(this.resultUrl)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.submitResultExportJob(jobRequest));
            logger.info("Started result export job id={}", jobId);

            return jobId;
        }
    }
}

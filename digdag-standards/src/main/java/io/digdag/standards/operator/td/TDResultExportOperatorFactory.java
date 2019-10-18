package io.digdag.standards.operator.td;

import com.google.inject.Inject;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.model.TDExportResultJobRequest;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
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
        private final String resultSettings;
        private final String resultConnection;

        private TDResultExportOperator(OperatorContext context)
        {
        super(context, env, systemConfig);
        Config params = request.getConfig();
        this.jobId = params.get("job_id", String.class);
        this.resultConnection = params.get("result_connection", String.class);

        UserSecretTemplate template = UserSecretTemplate.of(params.parseNested("result_settings").toString());
        this.resultSettings = template.format(context.getSecrets());
    }

        @Override
        protected String startJob(TDOperator op, String domainKey)
        {
            TDExportResultJobRequest jobRequest = TDExportResultJobRequest.builder()
                    .jobId(this.jobId)
                    .resultConnectionId(String.valueOf(op.lookupConnection(this.resultConnection)))
                    .resultConnectionSettings(this.resultSettings)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.submitResultExportJob(jobRequest));
            logger.info("Started result export job id={}", jobId);

            return jobId;
        }

        @Override
        public final TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(systemDefaultConfig, env, params, context.getSecrets().getSecrets("td"), false)) {
                return runTask(op);
            } catch (TDClientException ex) {
                throw propagateTDClientException(ex);
            }
        }
    }
}

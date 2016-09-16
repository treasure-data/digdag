package io.digdag.standards.operator.td;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class TdPartialDeleteOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdPartialDeleteOperatorFactory.class);
    private final Map<String, String> env;

    @Inject
    public TdPartialDeleteOperatorFactory(@Environment Map<String, String> env)
    {
        this.env = env;
    }

    public String getType()
    {
        return "td_partial_delete";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new TdPartialDeleteOperator(projectPath, request);
    }

    private class TdPartialDeleteOperator
            extends BaseTdJobOperator
    {
        private final Config params;
        private final String table;
        private final Instant from;
        private final Instant to;

        private TdPartialDeleteOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, env);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.table = params.get("_command", String.class);
            this.from = params.get("from", TimestampParam.class).getTimestamp().toInstant(request.getTimeZone());
            this.to = params.get("to", TimestampParam.class).getTimestamp().toInstant(request.getTimeZone());

            if (from.getEpochSecond() % 3600 != 0 || to.getEpochSecond() % 3600 != 0) {
                throw new ConfigException("'from' and 'to' parameters must be whole hours");
            }
        }

        @Override
        protected String startJob(TDOperator op, String domainKey)
        {
            String jobId = op.submitNewJobWithRetry(client ->
                    client.partialDelete(op.getDatabase(), table, from.getEpochSecond(), to.getEpochSecond(), domainKey).getJobId());
            logger.info("Started partial delete job id={}", jobId);
            return jobId;
        }
    }
}

package io.digdag.standards.operator.jdbc;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractJdbcOperator<C>
    extends BaseOperator
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final TemplateEngine templateEngine;
    protected String statusTableName;
    protected String statusTableSchema;
    protected DurationParam statusTableCleanupDuration;

    AbstractJdbcOperator(OperatorContext context, TemplateEngine templateEngine)
    {
        super(context);
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    @Override
    public boolean isBlocking()
    {
        return true;
    }

    protected abstract C configure(SecretProvider secrets, Config params);

    protected abstract JdbcConnection connect(C connectionConfig);

    protected abstract String type();

    protected List<String> nestedConfigKeys()
    {
        return ImmutableList.of(type());
    }

    protected boolean strictTransaction(Config params)
    {
        return params.get("strict_transaction", Boolean.class, true);
    }

    protected abstract TaskResult run(Config params, Config state, C connectionConfig);

    protected abstract SecretProvider getSecretsForConnectionConfig();

    @Override
    public TaskResult runTask()
    {
        Config params = request.getConfig();
        for (String nestedConfigKey : nestedConfigKeys()) {
            Config nested = request.getConfig().getNestedOrGetEmpty(nestedConfigKey);
            params = params.mergeDefault(nested);
        }
        Config state = request.getLastStateParams().deepCopy();
        C connectionConfig = configure(getSecretsForConnectionConfig(), params);

        if (strictTransaction(params)) {
            statusTableName = params.get("status_table", String.class, "__digdag_status");
            statusTableSchema = params.get("status_table_schema", String.class, null);
            statusTableCleanupDuration = params.get("status_table_cleanup", DurationParam.class,
                    DurationParam.of(Duration.ofHours(24)));
        }

        return run(params, state, connectionConfig);
    }
}

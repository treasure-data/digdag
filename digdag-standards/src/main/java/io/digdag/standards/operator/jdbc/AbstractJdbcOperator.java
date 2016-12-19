package io.digdag.standards.operator.jdbc;

import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class AbstractJdbcOperator<C>
    extends BaseOperator
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final TemplateEngine templateEngine;

    AbstractJdbcOperator(OperatorContext context, TemplateEngine templateEngine)
    {
        super(context);
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    protected abstract C configure(SecretProvider secrets, Config params);

    protected abstract JdbcConnection connect(C connectionConfig);

    protected abstract String type();

    protected boolean strictTransaction(Config params)
    {
        return params.get("strict_transaction", Boolean.class, true);
    }

    protected abstract TaskResult run(Config params, Config state, C connectionConfig);

    protected abstract SecretProvider getSecretsForConnectionConfig();

    @Override
    public TaskResult runTask()
    {
        Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty(type()));
        Config state = request.getLastStateParams().deepCopy();
        C connectionConfig = configure(getSecretsForConnectionConfig(), params);
        return run(params, state, connectionConfig);
    }
}

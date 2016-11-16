package io.digdag.standards.operator.pg;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretAccessList;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;


public class PgOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "pg";
    private final TemplateEngine templateEngine;

    @Inject
    public PgOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return OPERATOR_TYPE;
    }

    @Override
    public SecretAccessList getSecretAccessList()
    {
        return PgConnectionConfig.getSecretAccessList();
    }

    @Override
    public PgOperator newOperator(OperatorContext context)
    {
        return new PgOperator(context, templateEngine);
    }

    static class PgOperator
        extends AbstractJdbcJobOperator<PgConnectionConfig>
    {
        PgOperator(OperatorContext context, TemplateEngine templateEngine)
        {
            super(context, templateEngine);
        }

        @Override
        protected PgConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return PgConnectionConfig.configure(secrets, params);
        }

        @Override
        protected PgConnection connect(PgConnectionConfig connectionConfig)
        {
            return PgConnection.open(connectionConfig);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig()
        {
            return context.getSecrets().getSecrets(type());
        }
    }
}

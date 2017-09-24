package io.digdag.standards.operator.redshift;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift";
    private final Config systemConfig;
    private final TemplateEngine templateEngine;

    @Inject
    public RedshiftOperatorFactory(Config systemConfig, TemplateEngine templateEngine)
    {
        this.systemConfig = systemConfig;
        this.templateEngine = templateEngine;
    }

    @Override
    public String getType()
    {
        return OPERATOR_TYPE;
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new RedshiftOperator(systemConfig, context, templateEngine);
    }

    private static class RedshiftOperator
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        RedshiftOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
        {
            super(systemConfig, context, templateEngine);
        }

        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

        @Override
        protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig)
        {
            return RedshiftConnection.open(connectionConfig);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig()
        {
            return context.getSecrets().getSecrets("aws.redshift");
        }
    }
}

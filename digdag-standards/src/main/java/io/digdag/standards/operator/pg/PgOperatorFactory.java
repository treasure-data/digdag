package io.digdag.standards.operator.pg;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.AbstractJdbcOperator;
import java.nio.file.Path;

public class PgOperatorFactory
        implements OperatorFactory
{
    private final TemplateEngine templateEngine;

    @Inject
    public PgOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "pg";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new PgOperator(workspacePath, request, templateEngine);
    }

    public static class PgOperator
        extends AbstractJdbcOperator<PgConnectionConfig>
    {
        public PgOperator(Path workspacePath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(workspacePath, request, templateEngine);
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
    }
}

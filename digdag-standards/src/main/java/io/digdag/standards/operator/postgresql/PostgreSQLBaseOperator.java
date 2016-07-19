package io.digdag.standards.operator.postgresql;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.ImmutableJdbcConnectionConfig;
import io.digdag.standards.operator.jdbc.JdbcConnectionConfig;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class PostgreSQLBaseOperator
        extends BaseOperator
{
    protected final TemplateEngine templateEngine;

    public PostgreSQLBaseOperator(Path workspacePath, TemplateEngine templateEngine, TaskRequest request)
    {
        super(checkNotNull(workspacePath, "workspacePath"), checkNotNull(request, "request"));
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    protected PostgreSQLConnection getPostgreSQLConnection(Config params)
            throws SQLException
    {
        String database = params.get("database", String.class);
        Optional<String> schema = params.getOptional("schema", String.class);
        String host = params.get("host", String.class, "localhost");
        int port = params.get("port", int.class, 5432);
        String user = params.get("user", String.class);
        Optional<String> password = params.getOptional("password", String.class);
        boolean ssl = params.get("ssl", boolean.class, false);
        Optional<Integer> fetchSize = params.getOptional("fetch_size", Integer.class);
        Optional<Integer> loginTimeout = params.getOptional("login_timeout", Integer.class);
        Optional<Integer> connectionTimeout = params.getOptional("connection_timeout", Integer.class);
        Optional<Integer> socketTimeout = params.getOptional("socket_timeout", Integer.class);

        JdbcConnectionConfig jdbcConnectionConfig = ImmutableJdbcConnectionConfig.builder().
                database(database).
                schema(schema).
                host(host).
                port(port).
                user(user).
                password(password).
                ssl(ssl).
                fetchSize(fetchSize).
                loginTimeout(loginTimeout).
                connectionTimeout(connectionTimeout).
                socketTimeout(socketTimeout).
                build();

        return new PostgreSQLConnection(jdbcConnectionConfig);
    }

}

package io.digdag.standards.operator.postgresql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import org.postgresql.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PostgreSQLOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(PostgreSQLOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public PostgreSQLOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "postgresql";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new PostgreSQLOperator(workspacePath, request);
    }

    private class PostgreSQLOperator
            extends BaseOperator
    {
        public PostgreSQLOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        private String escapeIdent(String ident)
        {
            StringBuilder buf = new StringBuilder();
            boolean isFirst = true;
            for (String elm : ident.split("\\.")) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    buf.append(".");
                }

                try {
                    Utils.escapeIdentifier(buf, elm);
                }
                catch (SQLException e) {
                    logger.warn("Unexpected error occurred: ident=" + ident + ", elm=" + elm, e);
                    return ident;
                }
            }
            return buf.toString();
        }

        private void issueQuery(PostgreSQLQueryRequest req)
                throws SQLException, ClassNotFoundException
        {

            String url = String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/%s", req.host(), req.port(), req.database());
            Class.forName("org.postgresql.Driver");

            Properties props = new Properties();
            if (req.schema().isPresent()) {
                props.setProperty("currentSchema", req.schema().get());
            }
            // TODO: Make them configurable
            props.setProperty("loginTimeout", String.valueOf(30));
            props.setProperty("connectTimeout", String.valueOf(30));
            props.setProperty("socketTimeout", String.valueOf(1800));
            props.setProperty("tcpKeepAlive", "true");
            props.setProperty("ssl", String.valueOf(req.ssl()));
            props.setProperty("applicationName", "digdag");

            Connection conn = DriverManager.getConnection(url, props);
            conn.prepareStatement(req.query()).execute();
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("postgresql"));

            String database = params.get("database", String.class);
            Optional<String> schema = Optional.fromNullable(params.get("schema", String.class, null));
            String host = params.get("host", String.class, "localhost");
            int port = params.get("port", int.class, 5432);
            String user = params.get("user", String.class);
            Optional<String> password = Optional.fromNullable(params.get("password", String.class, null));
            boolean ssl = params.get("ssl", boolean.class, false);

            String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);
            Optional<PostgreSQLTableParam> insertInto = params.getOptional("insert_into", PostgreSQLTableParam.class);
            Optional<PostgreSQLTableParam> createTable = params.getOptional("create_table", PostgreSQLTableParam.class);
            if (insertInto.isPresent() && createTable.isPresent()) {
                throw new ConfigException("Setting both insert_into and create_table is invalid");
            }

            String stmt;

            if (insertInto.isPresent()) {
                stmt = insertCommandStatement("INSERT INTO " + escapeIdent(insertInto.get().toString()), query);
            }
            else if (createTable.isPresent()) {
                stmt = insertCommandStatement("CREATE TABLE " + escapeIdent(createTable.get().toString()) + " AS ", query);
            }
            else {
                stmt = query;
            }

            PostgreSQLQueryRequest req = ImmutablePostgreSQLQueryRequest.builder().
                    database(database).
                    schema(schema).
                    query(stmt).
                    host(host).
                    port(port).
                    user(user).
                    password(password).
                    ssl(ssl).
                    build();

            try {
                issueQuery(req);
            }
            catch (SQLException | ClassNotFoundException e) {
                // TODO: Create an exception class
                throw new RuntimeException("Failed to send a query: " + req, e);
            }

            return TaskResult.defaultBuilder(request).build();
        }
    }

    private final static Pattern INSERT_LINE_PATTERN = Pattern.compile("(\\A|\\r?\\n)\\-\\-\\s*DIGDAG_INSERT_LINE(?:(?!\\n|\\z).)*", Pattern.MULTILINE);

    private final static Pattern HEADER_COMMENT_BLOCK_PATTERN = Pattern.compile("\\A([\\r\\n\\t]*(?:(?:\\A|\\n)\\-\\-[^\\n]*)+)\\n?(.*)\\z", Pattern.MULTILINE);

    @VisibleForTesting
    static String insertCommandStatement(String command, String original)
    {
        // try to insert command at "-- DIGDAG_INSERT_LINE" line
        Matcher ml = INSERT_LINE_PATTERN.matcher(original);
        if (ml.find()) {
            return ml.replaceFirst(ml.group(1) + command);
        }

        // try to insert command after header comments so that job list page
        // shows comments rather than INSERT or other non-informative commands
        Matcher mc = HEADER_COMMENT_BLOCK_PATTERN.matcher(original);
        if (mc.find()) {
            return mc.group(1) + "\n" + command + "\n" + mc.group(2);
        }

        // add command at the head
        return command + "\n" + original;
    }
}

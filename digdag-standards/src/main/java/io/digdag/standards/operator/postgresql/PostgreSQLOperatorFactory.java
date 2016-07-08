package io.digdag.standards.operator.postgresql;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.ImmutableJdbcConnectionConfig;
import io.digdag.standards.operator.jdbc.JdbcColumn;
import io.digdag.standards.operator.jdbc.JdbcConnection;
import io.digdag.standards.operator.jdbc.JdbcConnectionConfig;
import io.digdag.standards.operator.jdbc.JdbcSchema;
import io.digdag.standards.operator.jdbc.QueryResultHandler;
import io.digdag.standards.operator.jdbc.TypeGroup;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PostgreSQLOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(PostgreSQLOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public PostgreSQLOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    public String getType()
    {
        return "postgresql";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new PostgreSQLOperator(workspacePath, templateEngine, request);
    }

    private static class PostgreSQLConnection
            extends JdbcConnection
    {
        public PostgreSQLConnection(JdbcConnectionConfig config)
                throws SQLException
        {
            super(config);
        }

        @Override
        protected String getClassName()
        {
            return "org.postgresql.Driver";
        }

        @Override
        protected String getProtocolName()
        {
            return "postgresql";
        }
    }

    private static class PostgreSQLOperator
            extends BaseOperator
    {
        private final TemplateEngine templateEngine;

        private final static Pattern INSERT_LINE_PATTERN = Pattern.compile("(\\A|\\r?\\n)\\-\\-\\s*DIGDAG_INSERT_LINE(?:(?!\\n|\\z).)*", Pattern.MULTILINE);

        private final static Pattern HEADER_COMMENT_BLOCK_PATTERN = Pattern.compile("\\A([\\r\\n\\t]*(?:(?:\\A|\\n)\\-\\-[^\\n]*)+)\\n?(.*)\\z", Pattern.MULTILINE);

        public PostgreSQLOperator(Path workspacePath, TemplateEngine templateEngine, TaskRequest request)
        {
            super(checkNotNull(workspacePath, "workspacePath"), checkNotNull(request, "request"));
            this.templateEngine = checkNotNull(templateEngine, "templateEngine");
        }

        private void issueQuery(JdbcConnectionConfig config, String query, Optional<QueryResultHandler> resultHandler)
                throws SQLException, ClassNotFoundException
        {
            PostgreSQLConnection connection = new PostgreSQLConnection(config);
            if (resultHandler.isPresent()) {
                connection.executeAndFetchResult(query, resultHandler.get());
            }
            else {
                connection.executeUpdate(query);
            }
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
            Optional<String> downloadFile = params.getOptional("download_file", String.class);
            if (downloadFile.isPresent() && (insertInto.isPresent() || createTable.isPresent())) {
                // query results become empty if INSERT INTO or CREATE TABLE query runs
                throw new ConfigException("download_file is invalid if insert_into or create_table is set");
            }

            String stmt;
            Optional<QueryResultHandler> queryResultHandler;

            if (insertInto.isPresent()) {
                stmt = insertCommandStatement("INSERT INTO " + JdbcConnection.escapeIdent(insertInto.get().toString()), query);
                queryResultHandler = Optional.absent();
            }
            else if (createTable.isPresent()) {
                stmt = insertCommandStatement("CREATE TABLE " + JdbcConnection.escapeIdent(createTable.get().toString()) + " AS ", query);
                queryResultHandler = Optional.absent();
            }
            else {
                stmt = query;
                queryResultHandler = Optional.of(new QueryResultHandler() {
                    private BufferedWriter out;
                    private JdbcSchema schema;

                    @Override
                    public void before()
                    {
                        try {
                            out = checkNotNull(workspace.newBufferedWriter(downloadFile.get(), UTF_8));
                        }
                        catch (IOException e) {
                            Throwables.propagate(e);
                        }
                    }

                    @Override
                    public void schema(JdbcSchema schema)
                    {
                        this.schema = schema;
                        try {
                            addCsvHeader(out, schema.getColumns().stream().map(JdbcColumn::getName).collect(Collectors.toList()));
                        }
                        catch (IOException e) {
                            Throwables.propagate(e);
                        }
                    }

                    @Override
                    public void handleRow(List<Object> row)
                    {
                        try {
                            addCsvRow(out, schema.getColumns().stream().map(JdbcColumn::getTypeGroup).collect(Collectors.toList()), row);
                        }
                        catch (IOException e) {
                            Throwables.propagate(e);
                        }
                    }

                    @Override
                    public void after()
                    {
                        if (out != null) {
                            try {
                                out.close();
                            }
                            catch (IOException e) {
                                logger.warn("Failed to close {}", out);
                            }
                        }
                    }
                });
            }

            JdbcConnectionConfig req = ImmutableJdbcConnectionConfig.builder().
                    database(database).
                    schema(schema).
                    host(host).
                    port(port).
                    user(user).
                    password(password).
                    ssl(ssl).
                    build();

            try {
                issueQuery(req, stmt, queryResultHandler);
            }
            catch (SQLException | ClassNotFoundException e) {
                // TODO: Create an exception class
                throw new RuntimeException("Failed to send a query: " + req, e);
            }

            return TaskResult.defaultBuilder(request).build();
        }

        // TODO: Move these methods to a helper class
        private void addCsvHeader(Writer out, List<String> columnNames)
                throws IOException
        {
            boolean first = true;
            for (String columnName : columnNames) {
                if (first) { first = false; }
                else { out.write(DELIMITER_CHAR); }
                addCsvText(out, columnName);
            }
            out.write("\r\n");
        }

        private void addCsvRow(Writer out, List<TypeGroup> typeGroups, List<Object> row)
                throws IOException
        {
            for (int i = 0; i < typeGroups.size(); i++) {
                if (i > 0) {
                    out.write(DELIMITER_CHAR);
                }
                Object v = row.get(i);
                if (typeGroups.get(i) == TypeGroup.STRING) {
                    addCsvText(out, v.toString());
                }
                else {
                    addCsvText(out, v.toString());
                }
            }
            out.write("\r\n");
        }

        private void addCsvText(Writer out, String value)
                throws IOException
        {
            out.write(escapeAndQuoteCsvValue(value));
        }

        private static final char DELIMITER_CHAR = ',';
        private static final char ESCAPE_CHAR = '"';
        private static final char QUOTE_CHAR = '"';

        private String escapeAndQuoteCsvValue(String v)
        {
            if (v.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                sb.append(QUOTE_CHAR);
                sb.append(QUOTE_CHAR);
                return sb.toString();
            }

            StringBuilder escapedValue = new StringBuilder();
            char previousChar = ' ';

            boolean isRequireQuote = false;

            for (int i = 0; i < v.length(); i++) {
                char c = v.charAt(i);

                if (c == QUOTE_CHAR) {
                    escapedValue.append(ESCAPE_CHAR);
                    escapedValue.append(c);
                    isRequireQuote = true;
                }
                else if (c == '\r') {
                    escapedValue.append('\n');
                    isRequireQuote = true;
                }
                else if (c == '\n') {
                    if (previousChar != '\r') {
                        escapedValue.append('\n');
                        isRequireQuote = true;
                    }
                }
                else if (c == DELIMITER_CHAR) {
                    escapedValue.append(c);
                    isRequireQuote = true;
                }
                else {
                    escapedValue.append(c);
                }
                previousChar = c;
            }

            if (isRequireQuote) {
                StringBuilder sb = new StringBuilder();
                sb.append(QUOTE_CHAR);
                sb.append(escapedValue);
                sb.append(QUOTE_CHAR);
                return sb.toString();
            }
            else {
                return escapedValue.toString();
            }
        }

        protected String insertCommandStatement(String command, String original)
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
}

package io.digdag.standards.operator.postgresql;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.CSVWriter;
import io.digdag.standards.operator.jdbc.ImmutableJdbcConnectionConfig;
import io.digdag.standards.operator.jdbc.JdbcColumn;
import io.digdag.standards.operator.jdbc.JdbcConnection;
import io.digdag.standards.operator.jdbc.JdbcConnectionConfig;
import io.digdag.standards.operator.jdbc.JdbcSchema;
import io.digdag.standards.operator.jdbc.QueryResultHandler;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
        protected String getDriverClassName()
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

        private enum QueryType {
            SELECT_ONLY,
            WITH_INSERT_INTO,
            WITH_CREATE_TABLE,
            WITH_UPDATE_TABLE
        }

        public PostgreSQLOperator(Path workspacePath, TemplateEngine templateEngine, TaskRequest request)
        {
            super(checkNotNull(workspacePath, "workspacePath"), checkNotNull(request, "request"));
            this.templateEngine = checkNotNull(templateEngine, "templateEngine");
        }

        private void issueQuery(JdbcConnectionConfig config, QueryType queryType, String query, Optional<QueryResultHandler> resultHandler, Optional<String> destTable)
                throws SQLException, ClassNotFoundException
        {
            PostgreSQLConnection connection = new PostgreSQLConnection(config);
            switch (queryType) {
                case SELECT_ONLY:
                    connection.executeQueryAndFetchResult(query, resultHandler.get());
                    break;
                case WITH_INSERT_INTO:
                    connection.executeQueryWithInsertInto(query, destTable.get());
                    break;
                case WITH_CREATE_TABLE:
                    connection.executeQueryWithCreateTable(query, destTable.get());
                    break;
                case WITH_UPDATE_TABLE:
                    connection.executeQueryWithUpdateTable(query, destTable.get());
                    break;
                default:
                    throw new IllegalStateException("Shouldn't reach here");
            }
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("postgresql"));

            String database = params.get("database", String.class);
            Optional<String> schema = params.getOptional("schema", String.class);
            String host = params.get("host", String.class, "localhost");
            int port = params.get("port", int.class, 5432);
            String user = params.get("user", String.class);
            Optional<String> password = params.getOptional("password", String.class);
            boolean ssl = params.get("ssl", boolean.class, false);

            String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);
            Optional<PostgreSQLTableParam> insertInto = params.getOptional("insert_into", PostgreSQLTableParam.class);
            Optional<PostgreSQLTableParam> createTable = params.getOptional("create_table", PostgreSQLTableParam.class);
            Optional<PostgreSQLTableParam> updateTable = params.getOptional("update_table", PostgreSQLTableParam.class);
            Optional<String> keyColumn = params.getOptional("key_column", String.class);
            int manipulateTableOperationCount = 0;
            if (insertInto.isPresent()) {
                manipulateTableOperationCount += 1;
            }
            if (createTable.isPresent()) {
                manipulateTableOperationCount += 1;
            }
            if (updateTable.isPresent()) {
                manipulateTableOperationCount += 1;
                if (!keyColumn.isPresent()) {
                    throw new ConfigException("key_column is required when update_table is used");
                }
            }
            if (manipulateTableOperationCount > 1) {
                throw new ConfigException("Can't use more than 2 of insert_into/create_table/update_table");
            }
            Optional<String> downloadFile = params.getOptional("download_file", String.class);
            if (downloadFile.isPresent() && manipulateTableOperationCount > 0) {
                throw new ConfigException("Can't use download_file with insert_into/create_table/update_table");
            }

            QueryType queryType;
            Optional<QueryResultHandler> queryResultHandler = Optional.absent();
            Optional<String> destTable = Optional.absent();

            if (insertInto.isPresent()) {
                queryType = QueryType.WITH_INSERT_INTO;
                destTable = Optional.of(insertInto.get().toString());
            }
            else if (createTable.isPresent()) {
                queryType = QueryType.WITH_CREATE_TABLE;
                destTable = Optional.of(createTable.get().toString());
            }
            else if (updateTable.isPresent()) {
                queryType = QueryType.WITH_UPDATE_TABLE;
                destTable = Optional.of(createTable.get().toString());
            }
            else {
                queryType = QueryType.SELECT_ONLY;
                if (downloadFile.isPresent()) {
                    queryResultHandler = Optional.of(getResultCsvDownloader(downloadFile.get()));
                }
                else {
                    queryResultHandler = Optional.of(getResultCsvPrinter());
                }
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
                issueQuery(req, queryType, query, queryResultHandler, destTable);
            }
            catch (SQLException | ClassNotFoundException e) {
                // TODO: Create an exception class
                throw new RuntimeException("Failed to send a query: " + req, e);
            }

            return TaskResult.defaultBuilder(request).build();
        }

        private QueryResultHandler getResultCsvPrinter()
        {
            return getResultWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
        }

        private QueryResultHandler getResultCsvDownloader(String downloadFile)
        {
            BufferedWriter out = null;
            try {
                out = checkNotNull(workspace.newBufferedWriter(downloadFile, UTF_8));
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
            return getResultWriter(out);
        }

        private QueryResultHandler getResultWriter(Writer writer)
        {
            BufferedWriter out = new BufferedWriter(writer);
            final CSVWriter csvWriter = new CSVWriter(out);

            return new QueryResultHandler()
            {
                private JdbcSchema schema;

                @Override
                public void before()
                {
                }

                @Override
                public void schema(JdbcSchema schema)
                {
                    this.schema = schema;
                    try {
                        csvWriter.addCsvHeader(schema.getColumns().stream().map(JdbcColumn::getName).collect(Collectors.toList()));
                    }
                    catch (IOException e) {
                        Throwables.propagate(e);
                    }
                }

                @Override
                public void handleRow(List<Object> row)
                {
                    try {
                        csvWriter.addCsvRow(schema.getColumns().stream().map(JdbcColumn::getTypeGroup).collect(Collectors.toList()), row);
                    }
                    catch (IOException e) {
                        Throwables.propagate(e);
                    }
                }

                @Override
                public void after()
                {
                    if (csvWriter != null) {
                        try {
                            csvWriter.close();
                        }
                        catch (Exception e) {
                            logger.warn("Failed to close {}", csvWriter);
                        }
                    }
                }
            };
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

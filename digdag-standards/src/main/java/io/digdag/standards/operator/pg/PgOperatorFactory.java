package io.digdag.standards.operator.pg;

import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
//import io.digdag.standards.operator.jdbc.CSVWriter;
//import io.digdag.standards.operator.jdbc.JdbcColumn;
//import io.digdag.standards.operator.jdbc.JdbcQueryHelper;
//import io.digdag.standards.operator.jdbc.JdbcQueryTxHelper;
//import io.digdag.standards.operator.jdbc.JdbcSchema;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TableReference;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.PersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.NoTransactionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PgOperatorFactory
        implements OperatorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(PgOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public PgOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    public String getType()
    {
        return "pg";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new PgOperator(workspacePath, request);
    }

    private class PgOperator
        extends BaseOperator
    {
        private static final String QUERY_ID = "queryId";
        private static final String QUERY_STATUS = "queryStatus";
        //private static final String QUERY_STATUS_RUNNING = "running";
        //private static final String QUERY_STATUS_FINISHED = "finished";

        //private Optional<String> queryStatus;
        //private Optional<QueryResultHandler> queryResultHandler;

        public PgOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("pg"));
            Config state = request.getLastStateParams().deepCopy();

            String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

            PgConnectionConfig connectionConfig = PgConnectionConfig.configure(params);

            Optional<TableReference> insertInto = params.getOptional("insert_into", TableReference.class);
            Optional<TableReference> createTable = params.getOptional("create_table", TableReference.class);

            int queryModifier = 0;
            if (insertInto.isPresent()) queryModifier++;
            if (createTable.isPresent()) queryModifier++;
            if (queryModifier > 1) {
                throw new ConfigException("Can't use both of insert_into and create_table");
            }


            Optional<String> downloadFile = params.getOptional("download_file", String.class);
            if (downloadFile.isPresent() && queryModifier > 0) {
                throw new ConfigException("Can't use download_file with insert_into or create_table");
            }
            // TODO support store_last_results

            boolean strictTransaction = params.get("strict_transaction", Boolean.class, true);

            Optional<String> statusTableName;
            if (strictTransaction) {
                statusTableName = params.getOptional("status_table", String.class);
            }
            else {
                statusTableName = Optional.absent();
            }

            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new pg task");
                String queryId = UUID.randomUUID().toString();
                state.set(QUERY_ID, queryId);
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            String queryId = state.get(QUERY_ID, String.class);

            try (PgConnection connection = PgConnection.open(connectionConfig)) {
                String statement;
                boolean statementMayReturnResults;
                if (insertInto.isPresent() || createTable.isPresent()) {
                    try {
                        connection.validateStatement(query);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Given query is invalid", ex);
                    }

                    if (insertInto.isPresent()) {
                        statement = connection.buildInsertStatement(query, insertInto.get());
                    }
                    else {
                        statement = connection.buildCreateTableStatement(query, createTable.get());
                    }

                    try {
                        connection.validateStatement(statement);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Given query is valid but failed to build INSERT INTO statement (query may include multiple statements or semicolon \";\"?)", ex);
                    }
                    statementMayReturnResults = false;
                }
                else {
                    statement = query;
                    try {
                        connection.validateStatement(statement);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Given query is invalid", ex);
                    }
                    statementMayReturnResults = true;
                }

                TransactionHelper txHelper;
                if (strictTransaction) {
                    txHelper = new PersistentTransactionHelper(connection, statusTableName);
                }
                else {
                    txHelper = new NoTransactionHelper();
                }
                txHelper.prepare();

                Optional<TaskResult> executed = txHelper.lockedTransaction(() -> {
                    try {
                        if (downloadFile.isPresent()) {
                            connection.executeReadOnlyQuery(statement, (results) -> downloadResultsToFile(results, downloadFile.get()));
                        }
                        else if (statementMayReturnResults) {
                            connection.executeScript(statement);
                        }
                        else {
                            connection.executeUpdate(statement);
                        }
                    }
                    catch (SQLException ex) {
                        throw sqlException("executing query", ex);
                    }

                    return TaskResult.defaultBuilder(request).build();
                });

                return executed.or(() -> {
                    try {
                        if (downloadFile.isPresent()) {
                            connection.executeReadOnlyQuery(statement, (results) -> downloadResultsToFile(results, downloadFile.get()));
                        }
                        else {
                            // query is already done. do nothing.
                        }
                    }
                    catch (SQLException ex) {
                        throw sqlException("executing query", ex);
                    }

                    return TaskResult.defaultBuilder(request).build();
                });
            }
            catch (SQLException ex) {
                throw sqlException("connecting to database", ex);
            }
        }

        private RuntimeException sqlException(String action, SQLException ex)
        {
            throw new RuntimeException("SQL error during " + action, ex);
        }

        private void downloadResultsToFile(JdbcResultSet results, String fileName)
        {
            // TODO
        }
    }
}

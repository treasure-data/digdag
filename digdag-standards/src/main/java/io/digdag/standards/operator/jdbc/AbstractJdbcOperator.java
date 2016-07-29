package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractJdbcOperator <C>
    extends BaseOperator
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TemplateEngine templateEngine;

    private static final String QUERY_ID = "queryId";
    private static final String COMPLETED_AT = "completedAt";

    public AbstractJdbcOperator(Path workspacePath, TaskRequest request, TemplateEngine templateEngine)
    {
        super(workspacePath, request);
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    protected abstract C configure(Config params);

    protected abstract JdbcConnection connect(C connectionConfig);

    @Override
    public TaskResult runTask()
    {
        Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("pg"));
        Config state = request.getLastStateParams().deepCopy();

        String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

        C connectionConfig = configure(params);

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

        boolean readOnlyMode = downloadFile.isPresent();  // or store_last_results == true

        boolean strictTransaction = params.get("strict_transaction", Boolean.class, true);

        String statusTableName;
        DurationParam statusTableCleanupDuration;
        if (strictTransaction) {
            statusTableName = params.get("status_table", String.class, "__digdag_status");
            statusTableCleanupDuration = params.get("status_table_cleanup", DurationParam.class,
                    DurationParam.of(Duration.ofHours(24)));
        }
        else {
            statusTableName = null;
            statusTableCleanupDuration = null;
        }

        UUID queryId;
        if (readOnlyMode) {
            // queryId is not used
            queryId = null;
        }
        else {
            if (state.has(COMPLETED_AT)) {
                // TODO: Deserialize as Instant.class using https://github.com/FasterXML/jackson-datatype-jsr310
                logger.info("This task is already finished: " + state.get(COMPLETED_AT, Object.class));
                return TaskResult.defaultBuilder(request).build();
            }

            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new pg task");
                queryId = UUID.randomUUID();
                state.set(QUERY_ID, queryId);
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            queryId = state.get(QUERY_ID, UUID.class);
        }

        try (JdbcConnection connection = connect(connectionConfig)) {
            Exception statementError = connection.validateStatement(query);
            if (statementError != null) {
                throw new ConfigException("Given query is invalid", statementError);
            }

            if (readOnlyMode) {
                if (downloadFile.isPresent()) {
                    connection.executeReadOnlyQuery(query, (results) -> downloadResultsToFile(results, downloadFile.get()));
                }
                else {
                    connection.executeReadOnlyQuery(query, (results) -> skipResults(results));
                }
                return TaskResult.defaultBuilder(request).build();
            }
            else {
                String statement;
                boolean statementMayReturnResults;
                if (insertInto.isPresent() || createTable.isPresent()) {
                    if (insertInto.isPresent()) {
                        statement = connection.buildInsertStatement(query, insertInto.get());
                    }
                    else {
                        statement = connection.buildCreateTableStatement(query, createTable.get());
                    }

                    Exception modifiedStatementError = connection.validateStatement(statement);
                    if (modifiedStatementError != null) {
                        throw new ConfigException("Given query is valid but failed to build INSERT INTO statement (this may happen if given query includes multiple statements or semicolon \";\"?)", modifiedStatementError);
                    }
                    statementMayReturnResults = false;
                    logger.debug("Running a modified statement: {}", statement);
                }
                else {
                    statement = query;
                    statementMayReturnResults = true;
                }

                TransactionHelper txHelper;
                if (strictTransaction) {
                    txHelper = connection.getStrictTransactionHelper(statusTableName,
                            statusTableCleanupDuration.getDuration());
                }
                else {
                    txHelper = new NoTransactionHelper();
                }

                txHelper.prepare();

                boolean executed = txHelper.lockedTransaction(queryId, () -> {
                    if (statementMayReturnResults) {
                        connection.executeScript(statement);
                    }
                    else {
                        connection.executeUpdate(statement);
                    }
                });

                if (!executed) {
                    logger.debug("Query is already completed according to status table. Skipping statement execution.");
                }

                try {
                    txHelper.cleanup();
                }
                catch (Exception ex) {
                    logger.warn("Error during cleaning up status table. Ignoring.", ex);
                }

                // It's possible that the query already finished successfully, but this task don't know that
                // if the previous connection with database was disconnected during waiting result.
                // So setting `completedAt` in state is needed even if the task was already finished.
                state.set(COMPLETED_AT, Instant.now());
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
        }
        catch (NotReadOnlyException ex) {
            throw new ConfigException("Query must be read-only if download_file is set", ex.getCause());
        }
        catch (DatabaseException ex) {
            // expected error that should suppress stacktrace by default
            String message = ex.getMessage();
            throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
        }
    }

    private void downloadResultsToFile(JdbcResultSet results, String fileName)
    {
        try (CsvWriter csvWriter = new CsvWriter(workspace.newBufferedWriter(fileName, UTF_8))) {
            List<String> columnNames = results.getColumnNames();
            csvWriter.addCsvHeader(columnNames);
            while (true) {
                List<Object> values = results.next();
                if (values == null) {
                    break;
                }
                List<String> row = values.stream().map(value -> {
                    if (value == null) {
                        return (String) value;
                    }
                    else if (value instanceof String) {
                        return (String) value;
                    }
                    else {
                        return value.toString();  // TODO use jackson to serialize?
                    }
                })
                .collect(Collectors.toList());
                csvWriter.addCsvRow(row);
            }
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private void skipResults(JdbcResultSet results)
    {
        while (results.next() != null)
            ;
    }
}

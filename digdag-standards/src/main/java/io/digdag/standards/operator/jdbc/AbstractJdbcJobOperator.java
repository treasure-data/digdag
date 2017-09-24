package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.ImmutableTaskResult;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractJdbcJobOperator<C>
    extends AbstractJdbcOperator<C>
{
    private static final String POLL_INTERVAL = "pollInterval";
    private static final int INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 1200;
    private static final String QUERY_ID = "queryId";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final int maxStoredResultSize;

    protected AbstractJdbcJobOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
    {
        super(context, templateEngine);
        Config config = systemConfig.getNestedOrGetEmpty("jdbc").deepCopy().merge(systemConfig.getNestedOrGetEmpty(type()));
        this.maxStoredResultSize = config.get("max_stored_result_size", int.class, 64 * 1024);
    }

    @Override
    protected TaskResult run(Config params, Config state, C connectionConfig)
    {
        String query = workspace.templateCommand(templateEngine, params, "query", UTF_8);

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

        boolean storeContent = params.get("store_content", boolean.class, false);
        if (storeContent && queryModifier > 0) {
            throw new ConfigException("Can't use store_content with insert_into or create_table");
        }
        if (downloadFile.isPresent() && storeContent) {
            throw new ConfigException("Can't use both download_file and store_content at once");
        }

        boolean readOnlyMode = downloadFile.isPresent() || storeContent;

        boolean strictTransaction = strictTransaction(params);

        UUID queryId;
        if (readOnlyMode) {
            // queryId is not used
            queryId = null;
        }
        else {
            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new {} task", type());
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
                ImmutableTaskResult.Builder builder = TaskResult.defaultBuilder(request);
                if (downloadFile.isPresent()) {
                    connection.executeReadOnlyQuery(query, (results) -> downloadResultsToFile(results, downloadFile.get()));
                }
                else if (storeContent) {
                    connection.executeReadOnlyQuery(query, (results) -> storeResultInTaskResult(builder, results));
                }
                else {
                    connection.executeReadOnlyQuery(query, (results) -> skipResults(results));
                }
                return builder.build();
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
                    txHelper = connection.getStrictTransactionHelper(
                            statusTableSchema, statusTableName, statusTableCleanupDuration.getDuration());
                }
                else {
                    txHelper = new NoTransactionHelper();
                }

                txHelper.prepare(queryId);

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

                return TaskResult.defaultBuilder(request).build();
            }
        }
        catch (NotReadOnlyException ex) {
            throw new ConfigException("Query must be read-only if download_file is set", ex.getCause());
        }
        catch (LockConflictException ex) {
            int pollingInterval = state.get(POLL_INTERVAL, Integer.class, INITIAL_POLL_INTERVAL);
            // Set next interval for exponential backoff
            state.set(POLL_INTERVAL, Math.min(pollingInterval * 2, MAX_POLL_INTERVAL));
            throw TaskExecutionException.ofNextPolling(pollingInterval, ConfigElement.copyOf(state));
        }
        catch (DatabaseException ex) {
            // expected error that should suppress stacktrace by default
            String message = String.format("%s [%s]", ex.getMessage(), ex.getCause().getMessage());
            throw new TaskExecutionException(message, ex);
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

    private void storeResultInTaskResult(ImmutableTaskResult.Builder builder, JdbcResultSet jdbcResultSet)
    {
        List<String> columnNames = jdbcResultSet.getColumnNames();

        ObjectMapper objectMapper = new ObjectMapper();
        StringBuilder buf = new StringBuilder();
        boolean isFirst = true;

        buf.append("[");
        while (true) {
            if (isFirst) {
                isFirst = false;
            }
            else {
                buf.append(',');
            }

            List<Object> values = jdbcResultSet.next();
            if (values == null) {
                break;
            }
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                map.put(columnNames.get(i), values.get(i));
            }
            try {
                buf.append(objectMapper.writeValueAsString(map));
            }
            catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        }
        buf.append("]");

        if (buf.length() > maxStoredResultSize) {
            throw new TaskExecutionException("Content is too large: " + buf.length() + " > " + maxStoredResultSize);
        }

        ConfigFactory cf = request.getConfig().getFactory();
        Config result = cf.create();
        // TODO: Revisit these codes later
        // Config taskState = result.getNestedOrSetEmpty(type());
        // taskState.set("last_content", buf.toString());
        builder.addResetStoreParams(ConfigKey.of(type(), "last_content"));

        builder.storeParams(result);
    }
}

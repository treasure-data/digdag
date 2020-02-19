package io.digdag.standards.operator.td;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.inject.Inject;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.td.TDOperator.SystemDefaultConfig;
import io.digdag.util.BaseOperator;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.concat;
import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.td.BaseTdJobOperator.propagateTDClientException;
import static java.util.Locale.ENGLISH;

public class TdDdlOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdDdlOperatorFactory.class);
    private final Map<String, String> env;
    private final DurationInterval retryInterval;
    private final SystemDefaultConfig systemDefaultConfig;
    private final BaseTDClientFactory clientFactory;


    @Inject
    public TdDdlOperatorFactory(@Environment Map<String, String> env, Config systemConfig, BaseTDClientFactory clientFactory)
    {
        this.env = env;
        this.retryInterval = TDOperator.retryInterval(systemConfig);
        this.systemDefaultConfig = TDOperator.systemDefaultConfig(systemConfig);
        this.clientFactory = clientFactory;
    }

    public String getType()
    {
        return "td_ddl";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new TdDdlOperator(context);
    }

    private class TdDdlOperator
            extends BaseOperator
    {
        private final TaskState state;

        public TdDdlOperator(OperatorContext context)
        {
            super(context);
            this.state = TaskState.of(request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            List<String> dropDatabaseList = params.parseListOrGetEmpty("drop_databases", String.class);
            List<String> createDatabaseList = params.parseListOrGetEmpty("create_databases", String.class);
            List<String> emptyDatabaseList = params.parseListOrGetEmpty("empty_databases", String.class);

            List<TableParam> dropTableList = params.parseListOrGetEmpty("drop_tables", TableParam.class);
            List<TableParam> createTableList = params.parseListOrGetEmpty("create_tables", TableParam.class);
            List<TableParam> emptyTableList = params.parseListOrGetEmpty("empty_tables", TableParam.class);

            List<RenameTableConfig> renameTableList = params.parseListOrGetEmpty("rename_tables", RenameTableConfig.class);

            List<Consumer<TDOperator>> operations = new ArrayList<>();

            for (String d : concat(dropDatabaseList, emptyDatabaseList)) {
                operations.add(op -> {
                    logger.info("Deleting TD database {}", d);
                    op.withDatabase(d).ensureDatabaseDeleted(d);
                });
            }
            for (String d : concat(createDatabaseList, emptyDatabaseList)) {
                operations.add(op -> {
                    logger.info("Creating TD database {}", op.getDatabase(), d);
                    op.withDatabase(d).ensureDatabaseCreated(d);
                });
            }
            for (TableParam t : concat(dropTableList, emptyTableList)) {
                operations.add(op -> {
                    logger.info("Deleting TD table {}.{}", op.getDatabase(), t);
                    op.withDatabase(t.getDatabase().or(op.getDatabase())).ensureTableDeleted(t.getTable());
                });
            }
            for (TableParam t : concat(createTableList, emptyTableList)) {
                operations.add(op -> {
                    logger.info("Creating TD table {}.{}", op.getDatabase(), t);
                    op.withDatabase(t.getDatabase().or(op.getDatabase())).ensureTableCreated(t.getTable());
                });
            }
            for (RenameTableConfig r : renameTableList) {
                TableParam from = r.getFromTable();
                String to = r.getToTable();
                if (to.contains(".")) {
                    // renaming a table across databases is not supported by the td's rest api itself
                    throw new ConfigException("'to' option of rename_tables can't include database name");
                }
                operations.add(op -> {
                    logger.info("Renaming TD table {}.{} -> {}", op.getDatabase(), from, to);
                    op.withDatabase(from.getDatabase().or(op.getDatabase())).ensureExistentTableRenamed(from.getTable(), to);
                });
            }

            SecretProvider secrets = context.getSecrets().getSecrets("td");

            try (TDOperator op = TDOperator.fromConfig(clientFactory, systemDefaultConfig, env, params, secrets)) {
                // make sure that all "from" tables exist so that ignoring 404 Not Found in
                // op.ensureExistentTableRenamed is valid.
                if (!renameTableList.isEmpty()) {
                    int checkOperation = state.params().get("rename_check_operation", int.class, 0);
                    for (int i = checkOperation; i < renameTableList.size(); i++) {
                        state.params().set("rename_check_operation", i);

                        RenameTableConfig r = renameTableList.get(i);
                        TableParam from = r.getFromTable();
                        String database = from.getDatabase().or(op.getDatabase());

                        boolean exists = pollingRetryExecutor(state, "rename_check_retry")
                                .retryUnless(TDOperator::isFailedBeforeSendClientException)
                                .withRetryInterval(retryInterval)
                                .withErrorMessage("Failed check existence of table %s.%s", database, from.getTable())
                                .run(s -> {
                                    try {
                                        return op.withDatabase(database).tableExists(from.getTable());
                                    }
                                    catch (TDClientHttpException ex) {
                                        if (TDOperator.isAuthenticationErrorException(ex)) {
                                            op.updateApikey(secrets);
                                        }
                                        throw ex;
                                    }
                                });
                        if (!exists) {
                            throw new ConfigException(String.format(ENGLISH,
                                        "Renaming table %s.%s doesn't exist", database, from.getTable()));
                        }
                    }
                }

                int operation = state.params().get("operation", int.class, 0);
                for (int i = operation; i < operations.size(); i++) {
                    state.params().set("operation", i);

                    Consumer<TDOperator> o = operations.get(i);
                    pollingRetryExecutor(state, "retry")
                            .retryUnless(TDOperator::isFailedBeforeSendClientException)
                            .withRetryInterval(retryInterval)
                            .withErrorMessage("DDL operation failed")
                            .runAction(s -> {
                                try {
                                    o.accept(op);
                                }
                                catch (TDClientHttpNotFoundException ex) {
                                    if (TDOperator.isAuthenticationErrorException(ex)) {
                                        op.updateApikey(secrets);
                                    }
                                    throw ex;
                                }
                            });
                }
            }
            catch (TDClientException ex) {
                throw propagateTDClientException(ex);
            }

            return TaskResult.empty(request);
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableRenameTableConfig.class)
    interface RenameTableConfig
    {
        @JsonProperty("from")
        TableParam getFromTable();

        @JsonProperty("to")
        String getToTable();
    }
}

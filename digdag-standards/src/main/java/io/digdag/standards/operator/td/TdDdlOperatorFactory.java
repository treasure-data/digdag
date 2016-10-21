package io.digdag.standards.operator.td;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.concat;
import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;

public class TdDdlOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdDdlOperatorFactory.class);
    private final Map<String, String> env;
    private final DurationInterval retryInterval;

    @Inject
    public TdDdlOperatorFactory(@Environment Map<String, String> env, Config systemConfig)
    {
        this.env = env;
        this.retryInterval = TDOperator.retryInterval(systemConfig);
    }

    public String getType()
    {
        return "td_ddl";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new TdDdlOperator(projectPath, request);
    }

    private class TdDdlOperator
            extends BaseOperator
    {
        private final TaskState state;

        public TdDdlOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.state = TaskState.of(request);
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("td.*");
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            List<String> dropDatabaseList = params.getListOrEmpty("drop_databases", String.class);
            List<String> createDatabaseList = params.getListOrEmpty("create_databases", String.class);
            List<String> emptyDatabaseList = params.getListOrEmpty("empty_databases", String.class);

            List<TableParam> dropTableList = params.getListOrEmpty("drop_tables", TableParam.class);
            List<TableParam> createTableList = params.getListOrEmpty("create_tables", TableParam.class);
            List<TableParam> emptyTableList = params.getListOrEmpty("empty_tables", TableParam.class);

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

            try (TDOperator op = TDOperator.fromConfig(env, params, ctx.secrets().getSecrets("td"))) {
                int operation = state.params().get("operation", int.class, 0);
                for (int i = operation; i < operations.size(); i++) {
                    state.params().set("operation", i);
                    Consumer<TDOperator> o = operations.get(i);
                    pollingRetryExecutor(state, "retry")
                            .retryUnless(TDOperator::isDeterministicClientException)
                            .withRetryInterval(retryInterval)
                            .withErrorMessage("DDL operation failed")
                            .runAction(s -> o.accept(op));

                }
            }

            return TaskResult.empty(request);
        }
    }
}

package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
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
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;

import static io.digdag.standards.operator.td.TdOperatorFactory.joinJob;

public class TdWaitTableOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdWaitTableOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public TdWaitTableOperatorFactory(TemplateEngine templateEngine, Config systemConfig)
    {
        super(systemConfig);
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "td_wait_table";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdWaitTableOperator(workspacePath, request);
    }

    private class TdWaitTableOperator
            extends BaseOperator
    {
        public TdWaitTableOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            int pollInterval = getPollInterval(params);

            TableParam table = params.get("_command", TableParam.class);

            int rows = params.get("rows", int.class, 0);

            boolean done = poll(params, table, rows);

            if (done) {
                return TaskResult.empty(request);
            }
            else {
                throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(request.getLastStateParams()));
            }
        }
    }

    private boolean poll(Config params, TableParam table, int expectedRows)
    {
        String engine = params.get("engine", String.class, "presto");

        String tableName;
        switch (engine) {
            case "presto":
                tableName = TDOperator.escapePrestoTableName(table);
                break;
            case "hive":
                tableName = TDOperator.escapeHiveTableName(table);
                break;
            default:
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
        }
        String query = "select count(1) from " + tableName + " limit " + expectedRows;

        try (TDOperator op = TDOperator.fromConfig(params)) {
            if (!op.tableExists(table.getTable())) {
                return false;
            }

            if (expectedRows <= 0) {
                return true;
            }

            int priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            int jobRetry = params.get("job_retry", int.class, 0);

            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .createTDJobRequest();

            TDJobOperator job = op.submitNewJob(req);
            logger.info("Started {} job id={}:\n{}", engine, job.getJobId(), query);

            joinJob(job);

            Optional<ArrayValue> firstRow = job.getResult(ite -> ite.hasNext() ? Optional.of(ite.next()) : Optional.absent());

            if (!firstRow.isPresent()) {
                throw new RuntimeException("Got unexpected empty result for count job: " + job.getJobId());
            }
            if (firstRow.get().size() != 1) {
                throw new RuntimeException("Got unexpected result row size for count job: " + firstRow.get().size());
            }
            Value count = firstRow.get().get(0);
            IntegerValue actualRows;
            try {
                actualRows = count.asIntegerValue();
            }
            catch (MessageTypeCastException e) {
                throw new RuntimeException("Got unexpected value type count job: " + count.getValueType());
            }
            return BigInteger.valueOf(expectedRows).compareTo(actualRows.asBigInteger()) <= 0;
        }
    }
}

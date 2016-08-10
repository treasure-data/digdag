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
import io.digdag.util.BaseOperator;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.file.Path;

public class TdWaitTableOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    public static final String POLL_JOB = "pollJob";
    private static Logger logger = LoggerFactory.getLogger(TdWaitTableOperatorFactory.class);

    private static final String TABLE_EXISTS = "table_exists";

    @Inject
    public TdWaitTableOperatorFactory(Config systemConfig)
    {
        super(systemConfig);
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
        private final Config params;
        private final TableParam table;
        private final int pollInterval;
        private final int tableExistencePollInterval;
        private final int rows;
        private final String engine;
        private final int priority;
        private final int jobRetry;
        private final Config state;

        private TdWaitTableOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.table = params.get("_command", TableParam.class);
            this.pollInterval = getPollInterval(params);
            this.tableExistencePollInterval = Integer.min(pollInterval, TABLE_EXISTENCE_API_POLL_INTERVAL);
            this.rows = params.get("rows", int.class, 0);
            this.engine = params.get("engine", String.class, "presto");
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }
            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.jobRetry = params.get("job_retry", int.class, 0);
            this.state = request.getLastStateParams().deepCopy();
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Check if table exists using rest api
                if (!state.get(TABLE_EXISTS, Boolean.class, false)) {
                    if (!op.tableExists(table.getTable())) {
                        throw TaskExecutionException.ofNextPolling(tableExistencePollInterval, ConfigElement.copyOf(state));
                    }

                    // If the table exists and there's no requirement on the number of rows we're done here.
                    if (rows <= 0) {
                        return TaskResult.empty(request);
                    }

                    // Store that the table has been observed to exist
                    state.set(TABLE_EXISTS, true);
                }

                TDJobOperator job = op.runJob(state, POLL_JOB, this::startJob);
                state.remove(POLL_JOB);

                // Fetch the job output to see if the row count condition was fulfilled
                logger.debug("fetching poll job result: {}", job.getJobId());
                boolean done = fetchJobResult(rows, job);

                // Go back to sleep if the row count condition was not fulfilled
                if (!done) {
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                // The row count condition was fulfilled. We're done.
                return TaskResult.empty(request);
            }
        }

        private boolean fetchJobResult(int rows, TDJobOperator job)
        {
            Optional<ArrayValue> firstRow = job.getResult(ite -> ite.hasNext() ? Optional.of(ite.next()) : Optional.absent());

            if (!firstRow.isPresent()) {
                throw new TaskExecutionException("Got unexpected empty result for count job: " + job.getJobId(), ConfigElement.empty());
            }
            ArrayValue row = firstRow.get();
            if (row.size() != 1) {
                throw new TaskExecutionException("Got unexpected result row size for count job: " + row.size(), ConfigElement.empty());
            }
            Value count = row.get(0);
            IntegerValue actualRows;
            try {
                actualRows = count.asIntegerValue();
            }
            catch (MessageTypeCastException e) {
                throw new TaskExecutionException("Got unexpected value type count job: " + count.getValueType(), ConfigElement.empty());
            }

            return BigInteger.valueOf(rows).compareTo(actualRows.asBigInteger()) <= 0;
        }

        private String startJob(TDOperator op, String domainKey)
        {
            String query = createQuery();

            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .createTDJobRequest();

            String jobId = op.submitNewJob(req);
            logger.info("Started {} job id={}:\n{}", engine, jobId, query);
            return jobId;
        }

        private String createQuery()
        {
            String tableName;
            String query;

            switch (engine) {
                case "presto":
                    tableName = TDOperator.escapePrestoTableName(table);
                    query = "select count(*) from " + tableName;
                    break;
                case "hive":
                    tableName = TDOperator.escapeHiveTableName(table);
                    query = "select count(*) from (select * from " + tableName + " limit " + rows + ") sub";
                    break;
                default:
                    throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }
            return query;
        }
    }
}

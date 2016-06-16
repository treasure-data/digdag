package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJob;
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
    private static Logger logger = LoggerFactory.getLogger(TdWaitTableOperatorFactory.class);

    private static final String JOB_ID = "jobId";

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
        private TdWaitTableOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            int pollInterval = getPollInterval(params);
            int tableExistencePollInterval = Integer.min(pollInterval, TABLE_EXISTENCE_API_POLL_INTERVAL);
            TableParam table = params.get("_command", TableParam.class);
            int rows = params.get("rows", int.class, 0);
            String engine = params.get("engine", String.class, "presto");

            Config state = request.getLastStateParams().deepCopy();

            try (TDOperator op = TDOperator.fromConfig(params)) {

                Optional<String> existingJobId = state.getOptional(JOB_ID, String.class);

                // Check if table exists using rest api (if the query job has not yet been started)
                if (!existingJobId.isPresent()) {
                    if (!op.tableExists(table.getTable())) {
                        throw TaskExecutionException.ofNextPolling(tableExistencePollInterval, ConfigElement.copyOf(state));
                    }

                    // If the table exists and there's no requirement on the number of rows we're done here.
                    if (rows <= 0) {
                        return TaskResult.empty(request);
                    }
                }

                // Start the query job
                if (!existingJobId.isPresent()) {
                    String jobId = startJob(params, table, rows, engine, op);
                    state.set(JOB_ID, jobId);
                    throw TaskExecutionException.ofNextPolling(JOB_STATUS_API_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Poll for query job status
                assert existingJobId.isPresent();
                TDJobOperator job = op.newJobOperator(existingJobId.get());
                TDJob jobInfo = job.getJobInfo();
                TDJob.Status status = jobInfo.getStatus();
                logger.debug("poll job status: {}: {}", existingJobId.get(), jobInfo);

                if (!status.isFinished()) {
                    throw TaskExecutionException.ofNextPolling(JOB_STATUS_API_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                logger.debug("fetching poll job result: {}", existingJobId.get());
                boolean done = fetchJobResult(rows, job);

                if (!done) {
                    state.remove(JOB_ID);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                return TaskResult.empty(request);
            }
        }

        private boolean fetchJobResult(int rows, TDJobOperator job)
        {
            Optional<ArrayValue> firstRow = job.getResult(ite -> ite.hasNext() ? Optional.of(ite.next()) : Optional.absent());

            if (!firstRow.isPresent()) {
                throw new RuntimeException("Got unexpected empty result for count job: " + job.getJobId());
            }
            ArrayValue row = firstRow.get();
            if (row.size() != 1) {
                throw new RuntimeException("Got unexpected result row size for count job: " + row.size());
            }
            Value count = row.get(0);
            IntegerValue actualRows;
            try {
                actualRows = count.asIntegerValue();
            }
            catch (MessageTypeCastException e) {
                throw new RuntimeException("Got unexpected value type count job: " + count.getValueType());
            }

            return BigInteger.valueOf(rows).compareTo(actualRows.asBigInteger()) <= 0;
        }

        private String startJob(Config params, TableParam table, int rows, String engine, TDOperator op)
        {
            String query = createQuery(table, rows, engine);

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
            String jobId = job.getJobId();
            logger.info("Started {} job id={}:\n{}", engine, jobId, query);
            return jobId;
        }

        private String createQuery(TableParam table, int rows, String engine)
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

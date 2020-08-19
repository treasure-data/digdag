package io.digdag.standards.operator.td;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.td.TDOperator.SystemDefaultConfig;
import io.digdag.util.AbstractWaitOperatorFactory;
import io.digdag.util.BaseOperator;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.td.BaseTdJobOperator.poolNameOfEngine;
import static io.digdag.standards.operator.td.BaseTdJobOperator.propagateTDClientException;

public class TdWaitTableOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    private static final int TABLE_EXISTENCE_API_POLL_INTERVAL = 30;

    private static Logger logger = LoggerFactory.getLogger(TdWaitTableOperatorFactory.class);

    private static final String EXISTS = "exists";
    private static final String RESULT = "result";

    private static final String TABLE_EXISTS = "table_exists";
    private static final String POLL_JOB = "pollJob";

    private final Map<String, String> env;
    private final DurationInterval pollInterval;
    private final DurationInterval retryInterval;
    private final SystemDefaultConfig systemDefaultConfig;
    private final BaseTDClientFactory clientFactory;

    @Inject
    public TdWaitTableOperatorFactory(Config systemConfig, @Environment Map<String, String> env, BaseTDClientFactory clientFactory)
    {
        super("td.wait", systemConfig);
        this.pollInterval = TDOperator.pollInterval(systemConfig);
        this.retryInterval = TDOperator.retryInterval(systemConfig);
        this.systemDefaultConfig = TDOperator.systemDefaultConfig(systemConfig);
        this.env = env;
        this.clientFactory = clientFactory;
    }

    public String getType()
    {
        return "td_wait_table";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new TdWaitTableOperator(context);
    }

    @VisibleForTesting
    class TdWaitTableOperator
            extends BaseOperator
    {
        private final Config params;
        private final TableParam table;
        private final int tablePollInterval;
        private final int tableExistencePollInterval;
        private final int rows;
        private final String engine;
        private final Optional<String> engineVersion;
        private final int priority;
        private final Optional<String> poolName;
        private final int jobRetry;
        private final TaskState state;

        private TdWaitTableOperator(OperatorContext context)
        {
            super(context);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.table = params.get("_command", TableParam.class);
            this.tablePollInterval = getPollInterval(params);
            this.tableExistencePollInterval = Integer.min(tablePollInterval, TABLE_EXISTENCE_API_POLL_INTERVAL);
            this.rows = params.get("rows", int.class, 0);
            this.engine = params.get("engine", String.class, "presto");
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }
            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.poolName = poolNameOfEngine(params, engine);
            this.jobRetry = params.get("job_retry", int.class, 0);
            this.state = TaskState.of(request);
            this.engineVersion = params.getOptional("engine_version", String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(clientFactory, systemDefaultConfig, env, params, context.getSecrets().getSecrets("td"))) {

                // Check if table exists using rest api
                if (!state.params().get(TABLE_EXISTS, Boolean.class, false)) {

                    if (!tableExists(op)) {
                        throw state.pollingTaskExecutionException(tableExistencePollInterval);
                    }

                    // If the table exists and there's no requirement on the number of rows we're done here.
                    if (rows <= 0) {
                        return TaskResult.empty(request);
                    }

                    // Store that the table has been observed to exist
                    state.params().set(TABLE_EXISTS, true);
                }

                TDJobOperator job = op.runJob(state, POLL_JOB, pollInterval, retryInterval, this::startJob);

                // Fetch the job output to see if the row count condition was fulfilled
                logger.debug("fetching poll job result: {}", job.getJobId());
                boolean done = fetchJobResult(rows, job);

                // Remove the poll job state _after_ fetching the result so that the result fetch can be retried without resubmitting the job.
                state.params().remove(POLL_JOB);

                // Go back to sleep if the row count condition was not fulfilled
                if (!done) {
                    throw state.pollingTaskExecutionException(tablePollInterval);
                }

                // The row count condition was fulfilled. We're done.
                return TaskResult.empty(request);
            }
            catch (TDClientException ex) {
                throw propagateTDClientException(ex);
            }
        }

        private boolean tableExists(TDOperator op)
        {

            return pollingRetryExecutor(state, EXISTS)
                    .retryUnless(TDOperator::isDeterministicClientException)
                    .withErrorMessage("Failed to check existence of table '%s.%s'", op.getDatabase(), table.getTable())
                    .withRetryInterval(retryInterval)
                    .run(s -> op.tableExists(table.getTable()));
        }

        private boolean fetchJobResult(int rows, TDJobOperator job)
        {
            Optional<ArrayValue> firstRow = pollingRetryExecutor(state, RESULT)
                    .retryUnless(TDOperator::isDeterministicClientException)
                    .withErrorMessage("Failed to download result of job '%s'", job.getJobId())
                    .withRetryInterval(retryInterval)
                    .run(s -> job.getResult(
                            ite -> ite.hasNext()
                                    ? Optional.of(ite.next())
                                    : Optional.absent()));

            if (!firstRow.isPresent()) {
                throw new TaskExecutionException("Got unexpected empty result for count job: " + job.getJobId());
            }
            ArrayValue row = firstRow.get();
            if (row.size() != 1) {
                throw new TaskExecutionException("Got unexpected result row size for count job: " + row.size());
            }
            Value count = row.get(0);
            IntegerValue actualRows;
            try {
                actualRows = count.asIntegerValue();
            }
            catch (MessageTypeCastException e) {
                throw new TaskExecutionException("Got unexpected value type count job: " + count.getValueType());
            }

            return BigInteger.valueOf(rows).compareTo(actualRows.asBigInteger()) <= 0;
        }

        @VisibleForTesting
        String startJob(TDOperator op, String domainKey)
        {
            String query = createQuery();
            Optional<String> ev = Optional.absent();

            // engine version is effective only for hive.
            if (engine.equals("hive")) {
                ev = engineVersion;
            }

            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .setPoolName(poolName.orNull())
                    .setDomainKey(domainKey)
                    .setEngineVersion(ev.transform(e -> TDJob.EngineVersion.fromString(e)).orNull())
                    .createTDJobRequest();

            String jobId = op.submitNewJobWithRetry(req);
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

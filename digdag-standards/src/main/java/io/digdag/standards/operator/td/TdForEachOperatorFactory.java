package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Limits;
import io.digdag.core.workflow.TaskLimitExceededException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdForEachOperatorFactory
        implements OperatorFactory
{
    private static final String JOB_ID = "jobId";
    private static final String DOMAIN_KEY = "domainKey";
    private static final String POLL_ITERATION = "pollIteration";

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30000;

    private static Logger logger = LoggerFactory.getLogger(TdForEachOperatorFactory.class);

    private final TemplateEngine templateEngine;
    private final ConfigFactory configFactory;

    @Inject
    public TdForEachOperatorFactory(TemplateEngine templateEngine, ConfigFactory configFactory, Config systemConfig)
    {
        this.templateEngine = templateEngine;
        this.configFactory = configFactory;
    }

    public String getType()
    {
        return "td_for_each";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdForEachOperator(workspacePath, request);
    }

    private class TdForEachOperator
            extends BaseOperator
    {
        private final Config params;
        private final String query;
        private final int priority;
        private final int jobRetry;
        private final String engine;
        private final Config state;
        private final Optional<String> existingJobId;
        private final Optional<String> existingDomainKey;

        private final Config doConfig;

        private TdForEachOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)

            this.jobRetry = params.get("job_retry", int.class, 0);

            this.engine = params.get("engine", String.class, "presto");

            this.state = request.getLastStateParams().deepCopy();

            this.existingJobId = state.getOptional(JOB_ID, String.class);
            this.existingDomainKey = state.getOptional(DOMAIN_KEY, String.class);

            this.doConfig = request.getConfig().getNested("_do");
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Generate and store domain key before starting the job
                if (!existingDomainKey.isPresent()) {
                    String domainKey = UUID.randomUUID().toString();
                    state.set(DOMAIN_KEY, domainKey);
                    throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
                }

                // Start the job
                if (!existingJobId.isPresent()) {
                    String jobId = startJob(op);
                    state.set(JOB_ID, jobId);
                    state.set(POLL_ITERATION, 1);
                    throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Check if the job is done
                String jobId = existingJobId.get();
                TDJobOperator job = op.newJobOperator(jobId);
                TDJobSummary status = job.checkStatus();
                boolean done = status.getStatus().isFinished();
                if (!done) {
                    int pollIteration = state.get(POLL_ITERATION, int.class, 1);
                    state.set(POLL_ITERATION, pollIteration + 1);
                    int pollInterval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, pollIteration), MAX_POLL_INTERVAL);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                // Get the job results
                return processJobResult(op, job, status);
            }
        }

        private TaskResult processJobResult(TDOperator op, TDJobOperator j, TDJobSummary summary)
        {
            List<Config> rows = fetchRows(j);

            boolean parallel = params.get("_parallel", boolean.class, false);

            Config subtasks = doConfig.getFactory().create();
            for (int i = 0; i < rows.size(); i++) {
                Config row = rows.get(i);
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").getNestedOrSetEmpty("td").getNestedOrSetEmpty("each").setAll(row);
                subtasks.set("+td-for-each-" + i, subtask);
            }

            if (parallel) {
                subtasks.set("_parallel", true);
            }

            return TaskResult.defaultBuilder(request)
                    .subtaskConfig(subtasks)
                    .build();
        }

        private String startJob(TDOperator op)
        {
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }

            assert existingDomainKey.isPresent();
            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .setDomainKey(existingDomainKey)
                    .setScheduledTime(request.getSessionTime().getEpochSecond())
                    .createTDJobRequest();

            TDJobOperator j = op.submitNewJob(req);
            logger.info("Started {} job id={}:\n{}", engine, j.getJobId(), query);

            return j.getJobId();
        }

        private List<Config> fetchRows(TDJobOperator job)
        {
            List<String> columnNames = job.getResultColumnNames();
            return job.getResult(ite -> {
                List<Config> rows = new ArrayList<>();
                while (ite.hasNext()) {
                    rows.add(row(columnNames, ite.next().asArrayValue()));
                    if (rows.size() > Limits.maxWorkflowTasks()) {
                        throw new TaskLimitExceededException("Too many tasks. Limit: " + Limits.maxWorkflowTasks());
                    }
                }
                return rows;
            });
        }

        private Config row(List<String> keys, ArrayValue values)
        {
            Config config = configFactory.create();
            // TODO: fail on keys and values count mismatch?
            int n = Math.min(keys.size(), values.size());
            for (int i = 0; i < n; i++) {
                config.set(keys.get(i), value(values.get(i)));
            }
            return config;
        }

        private Object value(Value value)
        {
            switch (value.getValueType()) {
                case NIL:
                    return null;
                case BOOLEAN:
                    return value.asBooleanValue().getBoolean();
                case INTEGER:
                    return value.asIntegerValue().toLong();
                case FLOAT:
                    return value.asFloatValue().toFloat();
                case STRING:
                    return value.asStringValue().toString();
                case BINARY:
                case ARRAY:
                case MAP:
                case EXTENSION:
                default:
                    throw new UnsupportedOperationException("Unsupported column type: " + value.getValueType());
            }
        }
    }
}

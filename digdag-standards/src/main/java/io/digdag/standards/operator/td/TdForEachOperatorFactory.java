package io.digdag.standards.operator.td;

import com.google.inject.Inject;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Environment;
import io.digdag.core.Limits;
import io.digdag.core.workflow.TaskLimitExceededException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TaskExecutionException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.td.TDOperator.isDeterministicClientException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdForEachOperatorFactory
        implements OperatorFactory
{
    private static final String RESULT = "result";
    private static final String RETRY = "retry";

    private static Logger logger = LoggerFactory.getLogger(TdForEachOperatorFactory.class);

    private final TemplateEngine templateEngine;
    private final ConfigFactory configFactory;
    private final Map<String, String> env;
    private final Config systemConfig;

    @Inject
    public TdForEachOperatorFactory(TemplateEngine templateEngine, ConfigFactory configFactory, @Environment Map<String, String> env, Config systemConfig)
    {
        this.templateEngine = templateEngine;
        this.configFactory = configFactory;
        this.env = env;
        this.systemConfig = systemConfig;
    }

    public String getType()
    {
        return "td_for_each";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new TdForEachOperator(projectPath, request);
    }

    private class TdForEachOperator
            extends BaseTdJobOperator
    {
        private final Config params;
        private final String query;
        private final int priority;
        private final int jobRetry;
        private final String engine;

        private final Config doConfig;

        private TdForEachOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, env, systemConfig);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.query = workspace.templateCommand(templateEngine, params, "query", UTF_8);
            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.jobRetry = params.get("job_retry", int.class, 0);
            this.engine = params.get("engine", String.class, "presto");
            this.doConfig = request.getConfig().getNested("_do");
        }

        @Override
        protected TaskResult processJobResult(TaskExecutionContext ctx, TDOperator op, TDJobOperator j)
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

        @Override
        protected String startJob(TaskExecutionContext ctx, TDOperator op, String domainkey)
        {
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }

            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .setDomainKey(domainkey)
                    .setScheduledTime(request.getSessionTime().getEpochSecond())
                    .createTDJobRequest();

            String jobId = op.submitNewJobWithRetry(req);
            logger.info("Started {} job id={}:\n{}", engine, jobId, query);

            return jobId;
        }

        private List<Config> fetchRows(TDJobOperator job)
        {
            Config resultState = state.getNestedOrSetEmpty(RESULT);

            try {
                List<String> columnNames = job.getResultColumnNames();
                List<Config> result = job.getResult(ite -> {
                    List<Config> rows = new ArrayList<>();
                    while (ite.hasNext()) {
                        rows.add(row(columnNames, ite.next().asArrayValue()));
                        if (rows.size() > Limits.maxWorkflowTasks()) {
                            TaskLimitExceededException cause = new TaskLimitExceededException("Too many tasks. Limit: " + Limits.maxWorkflowTasks());
                            throw new TaskExecutionException(cause, TaskExecutionException.buildExceptionErrorConfig(cause));
                        }
                    }
                    return rows;
                });

                // Clear retry state
                resultState.remove(RETRY);

                return result;
            }
            catch (UncheckedIOException | TDClientException e) {
                if (isDeterministicClientException(e)) {
                    throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
                }
                int retry = resultState.get(RETRY, int.class, 0);
                resultState.set(RETRY, retry + 1);
                int interval = (int) Math.min(pollingConfig.minRetryInterval().getSeconds() * Math.pow(2, retry), pollingConfig.maxRetryInterval().getSeconds());
                logger.warn("Failed to download result of job '{}', retrying in {} seconds", job.getJobId(), interval, e);
                throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
            }
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

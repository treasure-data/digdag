package io.digdag.standards.operator.td;

import com.google.inject.Inject;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Limits;
import io.digdag.core.workflow.TaskLimitExceededException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
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

import static java.nio.charset.StandardCharsets.UTF_8;

public class TdForEachOperatorFactory
        implements OperatorFactory
{
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
        private TdForEachOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

            Config doConfig = request.getConfig().getNested("_do");

            List<Config> rows = runQuery(request, params, query);

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

        private List<Config> runQuery(TaskRequest request, Config params, String query)
        {
            String engine = params.get("engine", String.class, "presto");
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }

            try (TDOperator op = TDOperator.fromConfig(params)) {

                int priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
                int jobRetry = params.get("job_retry", int.class, 0);

                TDJobRequest req = new TDJobRequestBuilder()
                        .setType(engine)
                        .setDatabase(op.getDatabase())
                        .setQuery(query)
                        .setRetryLimit(jobRetry)
                        .setPriority(priority)
                        .setScheduledTime(request.getSessionTime().getEpochSecond())
                        .createTDJobRequest();

                TDJobOperator j = op.submitNewJob(req);
                logger.info("Started {} job id={}:\n{}", engine, j.getJobId(), query);

                j.joinJob();

                return fetchRows(j);
            }
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

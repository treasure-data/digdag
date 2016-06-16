package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.TDClientHttpNotFoundException;
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
import org.msgpack.value.BooleanValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static io.digdag.standards.operator.td.TdOperatorFactory.joinJob;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdWaitOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdWaitOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public TdWaitOperatorFactory(TemplateEngine templateEngine, Config systemConfig)
    {
        super(systemConfig);
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "td_wait";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdWaitOperator(workspacePath, request);
    }

    private class TdWaitOperator
            extends BaseOperator
    {
        private TdWaitOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            int pollInterval = getPollInterval(params);

            String query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

            int rows = params.get("rows", int.class, 1);

            boolean done = runPollQuery(request, params, query, rows);

            if (done) {
                return TaskResult.empty(request);
            }
            else {
                throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(request.getLastStateParams()));
            }
        }
    }

    private boolean runPollQuery(TaskRequest request, Config params, String originalQuery, int rows)
    {
        String engine = params.get("engine", String.class, "presto");
        if (!engine.equals("presto") && !engine.equals("hive")) {
            throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
        }

        String query = "select count(*) >= " + rows +
                " from (select 1 from (" + originalQuery + ") raw limit " + rows + ") sub";

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

            TDJobOperator job = op.submitNewJob(req);
            logger.info("Started {} job id={}:\n{}", engine, job.getJobId(), query);

            joinJob(job);

            Optional<ArrayValue> firstRow = job.getResult(ite -> ite.hasNext() ? Optional.of(ite.next()) : Optional.absent());

            if (!firstRow.isPresent()) {
                throw new RuntimeException("Got unexpected empty result for poll job: " + job.getJobId());
            }
            ArrayValue row = firstRow.get();
            if (row.size() != 1) {
                throw new RuntimeException("Got unexpected result row size for poll job: " + row.size());
            }
            Value condition = row.get(0);
            try {
                return condition.asBooleanValue().getBoolean();
            }
            catch (MessageTypeCastException e) {
                throw new RuntimeException("Got unexpected value type count job: " + condition.getValueType());
            }

        }
    }
}

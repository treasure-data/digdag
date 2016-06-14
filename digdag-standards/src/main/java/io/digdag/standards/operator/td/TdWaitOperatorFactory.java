package io.digdag.standards.operator.td;

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

            int defaultRows = query.trim().isEmpty() ? 0 : 1;
            int rows = params.get("rows", int.class, defaultRows);

            boolean done = runPollQuery(request, params, query, rows);

            if (done) {
                return TaskResult.empty(request);
            }
            else {
                throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(request.getLastStateParams()));
            }
        }
    }

    private boolean runPollQuery(TaskRequest request, Config params, String query, int rows)
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

            TDJobOperator job = op.submitNewJob(req);
            logger.info("Started {} job id={}:\n{}", engine, job.getJobId(), query);

            joinJob(job);

            // TODO: wrap query in a query using count and limit instead of storing and downloading the actual full query result?

            try {
                return job.getResult(ite -> {
                    int n = 0;
                    while (true) {
                        if (n >= rows) {
                            return true;
                        }
                        if (!ite.hasNext()) {
                            return false;
                        }
                        ite.next();
                        n++;
                    }
                });
            }
            catch (TDClientHttpNotFoundException ex) {
                // this happens if query is INSERT or CREATE. return empty results
                return false;
            }
        }
    }
}

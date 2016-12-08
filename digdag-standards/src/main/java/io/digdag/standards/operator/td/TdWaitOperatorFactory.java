package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretAccessList;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.td.BaseTdJobOperator.configSelectorBuilder;
import static io.digdag.standards.operator.td.TDOperator.isDeterministicClientException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdWaitOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdWaitOperatorFactory.class);

    private static final String RESULT = "result";
    private static final String POLL_JOB = "pollJob";

    private final TemplateEngine templateEngine;
    private final Map<String, String> env;
    private final DurationInterval pollInterval;
    private final DurationInterval retryInterval;

    @Inject
    public TdWaitOperatorFactory(TemplateEngine templateEngine, Config systemConfig, @Environment Map<String, String> env)
    {
        super(systemConfig);
        this.templateEngine = templateEngine;
        this.env = env;
        this.pollInterval = TDOperator.pollInterval(systemConfig);
        this.retryInterval = TDOperator.retryInterval(systemConfig);
    }

    public String getType()
    {
        return "td_wait";
    }

    @Override
    public SecretAccessList getSecretAccessList()
    {
        return configSelectorBuilder()
            .build();
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new TdWaitOperator(context);
    }

    private class TdWaitOperator
            extends BaseOperator
    {
        private final Config params;
        private final String query;
        private final int queryPollInterval;
        private final String engine;
        private final int priority;
        private final int jobRetry;
        private final TaskState state;

        private TdWaitOperator(OperatorContext context)
        {
            super(context);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.query = workspace.templateCommand(templateEngine, params, "query", UTF_8);
            this.queryPollInterval = getPollInterval(params);
            this.engine = params.get("engine", String.class, "presto");
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }
            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.jobRetry = params.get("job_retry", int.class, 0);
            this.state = TaskState.of(request);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(env, params, context.getSecrets().getSecrets("td"))) {

                TDJobOperator job = op.runJob(state, POLL_JOB, pollInterval, retryInterval, this::startJob);

                // Fetch the job output to see if the query condition has been fulfilled
                logger.debug("fetching poll job result: {}", job.getJobId());
                boolean done = fetchJobResult(job);

                // Remove the poll job state _after_ fetching the result so that the result fetch can be retried without resubmitting the job.
                state.params().remove(POLL_JOB);

                // If the query condition was not fulfilled, go back to sleep.
                if (!done) {
                    throw state.pollingTaskExecutionException(queryPollInterval);
                }

                // The query condition was fulfilled, we're done.
                return TaskResult.empty(request);
            }
        }

        private String startJob(TDOperator op, String domainKey)
        {
            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .setScheduledTime(request.getSessionTime().getEpochSecond())
                    .setDomainKey(domainKey)
                    .createTDJobRequest();

            String jobId = op.submitNewJobWithRetry(req);
            logger.info("Started {} job id={}:\n{}", engine, jobId, query);

            return jobId;
        }

        private boolean fetchJobResult(TDJobOperator job)
        {
            Optional<ArrayValue> firstRow = pollingRetryExecutor(state, RESULT)
                    .retryUnless(TDOperator::isDeterministicClientException)
                    .withErrorMessage("Failed to download result of job '%s'", job.getJobId())
                    .run(s -> job.getResult(
                            ite -> ite.hasNext()
                                    ? Optional.of(ite.next())
                                    : Optional.absent()));

            // There must be at least one row in the result for the wait condition to be fulfilled.
            if (!firstRow.isPresent()) {
                return false;
            }

            ArrayValue row = firstRow.get();
            if (row.size() < 1) {
                throw new TaskExecutionException("Got empty row in result of query", ConfigElement.empty());
            }

            Value firstCol = row.get(0);
            return isTruthy(firstCol);
        }

        private boolean isTruthy(Value firstCol)
        {
            // Anything that is not NULL and not FALSE or 0 is considered truthy.
            switch (firstCol.getValueType()) {
                case NIL:
                    return false;
                case BOOLEAN:
                    return firstCol.asBooleanValue().getBoolean();
                case INTEGER:
                    return firstCol.asIntegerValue().asLong() != 0;
                default:
                    return true;
            }
        }
    }
}

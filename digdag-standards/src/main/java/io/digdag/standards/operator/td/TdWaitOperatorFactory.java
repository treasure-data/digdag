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
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static com.treasuredata.client.model.TDJob.Status.SUCCESS;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdWaitOperatorFactory
        extends AbstractWaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdWaitOperatorFactory.class);

    private static final String JOB_ID = "jobId";

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
        private final Config params;
        private final String query;
        private final int pollInterval;
        private final String engine;
        private final int priority;
        private final int jobRetry;
        private final Config state;
        private final Optional<String> existingJobId;

        private TdWaitOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));
            this.query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);
            this.pollInterval = getPollInterval(params);
            this.engine = params.get("engine", String.class, "presto");
            if (!engine.equals("presto") && !engine.equals("hive")) {
                throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): " + engine);
            }
            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.jobRetry = params.get("job_retry", int.class, 0);
            this.state = request.getLastStateParams().deepCopy();
            this.existingJobId = state.getOptional(JOB_ID, String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Start the query job
                if (!existingJobId.isPresent()) {
                    String jobId = startJob(op);
                    state.set(JOB_ID, jobId);
                    throw TaskExecutionException.ofNextPolling(JOB_STATUS_API_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Poll for query job status
                assert existingJobId.isPresent();

                TDJobOperator job = op.newJobOperator(existingJobId.get());
                TDJob jobInfo = job.getJobInfo();
                TDJob.Status status = jobInfo.getStatus();
                logger.debug("poll job status: {}: {}", existingJobId.get(), jobInfo);

                // Wait some more if the job is not yet finished
                if (!status.isFinished()) {
                    throw TaskExecutionException.ofNextPolling(JOB_STATUS_API_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Fail the task if the job failed
                if (status != SUCCESS) {
                    String message = jobInfo.getCmdOut() + "\n" + jobInfo.getStdErr();
                    throw new TaskExecutionException(message, ConfigElement.empty());
                }

                // Fetch the job output to see if the query condition has been fulfilled
                logger.debug("fetching poll job result: {}", existingJobId.get());
                boolean done = fetchJobResult(job);

                // If the query condition was not fulfilled, go back to sleep.
                if (!done) {
                    state.remove(JOB_ID);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                // The query condition was fulfilled, we're done.
                return TaskResult.empty(request);
            }
        }

        private String startJob(TDOperator op)
        {
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

            return job.getJobId();
        }

        private boolean fetchJobResult(TDJobOperator job)
        {
            Optional<ArrayValue> firstRow = job.getResult(ite -> ite.hasNext() ? Optional.of(ite.next()) : Optional.absent());

            // There must be at least one row in the result for the wait condition to be fulfilled.
            if (!firstRow.isPresent()) {
                return false;
            }

            ArrayValue row = firstRow.get();
            if (row.size() < 1) {
                throw new TaskExecutionException("Got empty row in result of query", ConfigElement.empty());
            }

            // Wait condition is fulfilled if the first column is not NULL and not FALSE.
            Value firstCol = row.get(0);
            return !firstCol.isNilValue() &&
                    !(firstCol.isBooleanValue() && !firstCol.asBooleanValue().getBoolean());
        }
    }
}

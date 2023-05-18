package io.digdag.standards.operator.gcp;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.Maps;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.operator.state.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

class BqJobRunner
{
    private static Logger logger = LoggerFactory.getLogger(BqJobRunner.class);

    private static final int MAX_JOB_ID_LENGTH = 1024;

    private static final String JOB_ID = "jobId";
    private static final String START = "start";
    private static final String RUNNING = "running";
    private static final String CHECK = "check";

    private final TaskRequest request;
    private final BqClient bq;
    private final TaskState state;
    private final String projectId;

    BqJobRunner(TaskRequest request, BqClient bq, String projectId)
    {
        this.request = Objects.requireNonNull(request, "request");
        this.bq = Objects.requireNonNull(bq, "bq");
        this.state = TaskState.of(request);
        this.projectId = Objects.requireNonNull(projectId, "projectId");
    }

    Job runJob(JobConfiguration config, Optional<String> location)
    {
        TaskState state = this.state.nestedState("commandStatus");
        // Generate job id
        Optional<String> jobId = state.params().getOptional(JOB_ID, String.class);
        if (!jobId.isPresent()) {
            state.params().set(JOB_ID, uniqueJobId());
            throw state.pollingTaskExecutionException(0);
        }
        String canonicalJobId = projectId + ":" + jobId.get();

        JobReference reference = new JobReference()
                .setProjectId(projectId)
                .setJobId(jobId.get());

        // Start job
        pollingRetryExecutor(state, START)
                .withErrorMessage("BigQuery job submission failed: %s", canonicalJobId)
                .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                .runOnce((TaskState s) -> {
                    logger.info("Submitting BigQuery job: {}", canonicalJobId);
                    Job job = new Job()
                            .setJobReference(reference)
                            .setConfiguration(config);

                    try {
                        bq.submitJob(projectId, job);
                    }
                    catch (GoogleJsonResponseException e) {
                        if (e.getStatusCode() == 409) {
                            // Already started
                            logger.debug("BigQuery job already started: {}", canonicalJobId, e);
                            return;
                        }
                        throw e;
                    }
                });

        // Wait for job to complete
        Job completed = pollingWaiter(state, RUNNING)
                .withWaitMessage("BigQuery job still running: %s", jobId.get())
                .awaitOnce(Job.class, pollState -> {

                    // Check job status
                    Job job = pollingRetryExecutor(pollState, CHECK)
                            .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                            .withErrorMessage("BigQuery job status check failed: %s", canonicalJobId)
                            .run(s -> {
                                logger.info("Checking BigQuery job status: {}", canonicalJobId);
                                return bq.jobStatus(projectId, jobId.get(), location);
                            });

                    // Done yet?
                    JobStatus status = job.getStatus();
                    switch (status.getState()) {
                        case "DONE":
                            return Optional.of(job);
                        case "PENDING":
                        case "RUNNING":
                            return Optional.absent();
                        default:
                            throw new TaskExecutionException("Unknown job state: " + canonicalJobId + ": " + status.getState());
                    }
                });

        // Check job result
        JobStatus status = completed.getStatus();
        if (status.getErrorResult() != null) {
            // Failed
            logger.error("BigQuery job failed: {}", canonicalJobId);
            for (ErrorProto error : status.getErrors()) {
                logger.error(toPrettyString(error));
            }
            throw new TaskExecutionException("BigQuery job failed: " + canonicalJobId, errorProperties(status.getErrors()));
        }

        // Success
        logger.info("BigQuery job successfully done: {}", canonicalJobId);

        return completed;
    }

    private static Map<String, String> errorProperties(List<ErrorProto> errors)
    {
        return ImmutableMap.of(
                "errors", errors.stream()
                        .map(BqJobRunner::toPrettyString)
                        .collect(Collectors.joining(", ")));
    }

    private static String toPrettyString(ErrorProto error)
    {
        try {
            return error.toPrettyString();
        }
        catch (IOException e) {
            return "<json error>";
        }
    }

    private String uniqueJobId()
    {
        String suffix = "_" + UUID.randomUUID().toString();
        String prefix = "digdag" +
                "_s" + request.getSiteId() +
                "_p_" + truncate(request.getProjectName().or(""), 256) +
                "_w_" + truncate(request.getWorkflowName(), 256) +
                "_t_" + request.getTaskId() +
                "_a_" + request.getAttemptId();
        int maxPrefixLength = MAX_JOB_ID_LENGTH - suffix.length();
        return truncate(prefix, maxPrefixLength) + suffix;
    }

    private static String truncate(String s, int n)
    {
        return s.substring(0, Math.min(s.length(), n));
    }

}

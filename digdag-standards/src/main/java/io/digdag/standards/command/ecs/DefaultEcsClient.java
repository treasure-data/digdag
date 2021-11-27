package io.digdag.standards.command.ecs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.RetryUtils;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.AccessDeniedException;
import com.amazonaws.services.ecs.model.BlockedException;
import com.amazonaws.services.ecs.model.ClusterNotFoundException;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.InvalidParameterException;
import com.amazonaws.services.ecs.model.ListTagsForResourceRequest;
import com.amazonaws.services.ecs.model.ListTagsForResourceResult;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsRequest;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsResult;
import com.amazonaws.services.ecs.model.PlatformTaskDefinitionIncompatibilityException;
import com.amazonaws.services.ecs.model.PlatformUnknownException;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskSetNotFoundException;
import com.amazonaws.services.ecs.model.UnsupportedFeatureException;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import io.digdag.client.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultEcsClient
        implements EcsClient
{
    private static Logger logger = LoggerFactory.getLogger(EcsClient.class);

    private final EcsClientConfig config;
    private final AmazonECSClient client;
    private final AWSLogs logs;

    private final int rateLimitMaxRetry;
    private final long rateLimitMaxJitterSecs;   // 0.0 <= jitterSecs < rateLimitBaseJitterSecs
    private final long rateLimitMaxBaseWaitSecs; // Max baseWaitSecs.
    private final long rateLimitBaseIncrementalSecs;
    private final int retryDelayOnAgentErrorSecs;

    protected DefaultEcsClient(
            final EcsClientConfig config,
            final AmazonECSClient client,
            final AWSLogs logs)
    {
        this(config, client, logs, 60, 10, 50, 10, 3);
    }

    protected DefaultEcsClient(
            final EcsClientConfig config,
            final AmazonECSClient client,
            final AWSLogs logs,
            final int rateLimitMaxRetry,
            final long rateLimitMaxJitterSecs,
            final long rateLimitMaxBaseWaitSecs,
            final long rateLimitBaseIncrementalSecs,
            final int retryDelayOnAgentErrorSecs
            )
    {
        this.config = config;
        this.client = client;
        this.logs = logs;
        this.rateLimitMaxRetry = rateLimitMaxRetry;
        this.rateLimitMaxJitterSecs = rateLimitMaxJitterSecs;
        this.rateLimitMaxBaseWaitSecs = rateLimitMaxBaseWaitSecs;
        this.rateLimitBaseIncrementalSecs = rateLimitBaseIncrementalSecs;
        this.retryDelayOnAgentErrorSecs = retryDelayOnAgentErrorSecs;
    }

    @Override
    public EcsClientConfig getConfig()
    {
        return config;
    }

    private RunTaskResult runTask(final RunTaskRequest request)
            throws ConfigException
    {
        try {
            return retryOnRateLimit(() -> client.runTask(request));
        }
        catch (InvalidParameterException |
                ClusterNotFoundException |
                UnsupportedFeatureException |
                PlatformUnknownException |
                PlatformTaskDefinitionIncompatibilityException |
                AccessDeniedException |
                BlockedException e) {
            throw new ConfigException("Task failed during the task submission", e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isAgentError(final RunTaskResult result)
    {
        for (final Failure f : result.getFailures()) {
            if (f.getReason().equals("AGENT")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Run task on AWS ECS.
     * Retry once on AGENT error, which can happen as a part of the agent's normal operation.
     * https://docs.aws.amazon.com/cli/latest/reference/ecs/run-task.html
     * https://docs.aws.amazon.com/AmazonECS/latest/developerguide/api_failures_messages.html
     *
     * @param request
     * @return
     */
    @Override
    public RunTaskResult submitTask(final RunTaskRequest request)
    {
        RunTaskResult result = runTask(request);
        if (isAgentError(result)) {
            logger.debug("Submitting a task failed with AGENT error. Will be retried in {} sec.", String.valueOf(retryDelayOnAgentErrorSecs));
            waitWithRandomJitter(retryDelayOnAgentErrorSecs, 0);
            result = runTask(request);
        }
        return result;
    }

    @Override
    public TaskDefinition getTaskDefinition(final String taskDefinitionArn)
    {
        final DescribeTaskDefinitionRequest request = new DescribeTaskDefinitionRequest()
                .withTaskDefinition(taskDefinitionArn);
        final DescribeTaskDefinitionResult result;
        try {
            result = retryOnRateLimit(() -> client.describeTaskDefinition(request));
        }
        catch (InvalidParameterException e) {
            throw new ConfigException("Task definition arn not found: " + taskDefinitionArn, e);
        }
        return result.getTaskDefinition();
    }

    @Override
    public List<Tag> getTaskDefinitionTags(final String taskDefinitionArn)
    {
        final ListTagsForResourceRequest tagsRequest = new ListTagsForResourceRequest()
                .withResourceArn(taskDefinitionArn);
        final ListTagsForResourceResult tagsResult = retryOnRateLimit(() -> client.listTagsForResource(tagsRequest)); // several runtime exception
        return tagsResult.getTags();
    }

    @Override
    public Optional<TaskDefinition> getTaskDefinitionByTags(final List<Tag> expectedTags)
    {
        String nextToken = null;
        while (true) {
            final ListTaskDefinitionsRequest listRequest = new ListTaskDefinitionsRequest();
            if (nextToken != null) {
                listRequest.withNextToken(nextToken);
            }

            final ListTaskDefinitionsResult listResult = retryOnRateLimit(() -> client.listTaskDefinitions(listRequest));
            final List<String> taskDefinitionArns = listResult.getTaskDefinitionArns();
            for (final String taskDefinitionArn : taskDefinitionArns) {
                final List<Tag> tags = getTaskDefinitionTags(taskDefinitionArn);
                final Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));

                boolean tagsMatched = false;
                for (final Tag expectedTag : expectedTags) {
                    final String expectedTagKey = expectedTag.getKey();
                    if (tagMap.containsKey(expectedTagKey)
                            && tagMap.get(expectedTagKey).equals(expectedTag.getValue())) {
                        tagsMatched = true;
                    }
                    else {
                        tagsMatched = false;
                        break;
                    }
                }

                if (tagsMatched) {
                    final TaskDefinition td = getTaskDefinition(taskDefinitionArn);
                    return Optional.of(td);
                }
            }
            nextToken = listResult.getNextToken();
            if (nextToken == null) {
                return Optional.absent();
            }
        }
    }

    @Override
    public Optional<TaskDefinition> getTaskDefinitionByTags(final Predicate<List<Tag>> condition)
    {
        String nextToken = null;
        while (true) {
            final ListTaskDefinitionsRequest listRequest = new ListTaskDefinitionsRequest();
            if (nextToken != null) {
                listRequest.withNextToken(nextToken);
            }

            final ListTaskDefinitionsResult listResult = retryOnRateLimit(() -> client.listTaskDefinitions(listRequest));
            final List<String> taskDefinitionArns = listResult.getTaskDefinitionArns();
            for (final String taskDefinitionArn : taskDefinitionArns) {
                final List<Tag> tags = getTaskDefinitionTags(taskDefinitionArn);
                if (condition.apply(tags)) {
                    return Optional.of(getTaskDefinition(taskDefinitionArn));
                }
            }
            nextToken = listResult.getNextToken();
            if (nextToken == null) {
                break;
            }
        }

        return Optional.absent();
    }

    /**
     * Get task running on AWS ECS.
     * https://docs.aws.amazon.com/cli/latest/reference/ecs/describe-tasks.html
     *
     * @param cluster
     * @param taskArn
     * @return
     */
    @Override
    public Task getTask(final String cluster, final String taskArn)
    {
        final DescribeTasksRequest request = new DescribeTasksRequest()
                .withCluster(cluster)
                .withTasks(taskArn);

        final DescribeTasksResult result;
        try {
            result = retryOnRateLimit(() -> client.describeTasks(request)); // several exceptions
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<Task> tasks = result.getTasks();
        if (tasks.isEmpty()) {
            throw new TaskSetNotFoundException(String.format(Locale.ENGLISH, "Task '%s' not found on cluster '%s'", taskArn, cluster));
        }
        return tasks.get(0);
    }

    /**
     * Stop task.
     * https://docs.aws.amazon.com/cli/latest/reference/ecs/stop-task.html
     *
     * @param cluster
     * @param taskArn
     */
    @Override
    public void stopTask(final String cluster, final String taskArn)
    {
        final StopTaskRequest request = new StopTaskRequest()
                .withCluster(cluster)
                .withTask(taskArn);
        retryOnRateLimit(() -> client.stopTask(request));
    }

    /**
     * Get log events.
     * https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html
     *
     * @param groupName
     * @param streamName
     * @param nextToken
     * @return
     */
    @Override
    public GetLogEventsResult getLog(
            final String groupName,
            final String streamName,
            final Optional<String> nextToken)
    {
        final GetLogEventsRequest request = new GetLogEventsRequest()
                // This should be true when using `nextToken`. See the doc for details.
                .withStartFromHead(true)
                .withLogGroupName(groupName)
                .withLogStreamName(streamName);
        if (nextToken.isPresent()) {
            request.withNextToken("f/" + nextToken.get());
        }
        return retryOnRateLimit(() -> logs.getLogEvents(request));
    }

    @Override
    public void close()
            throws IOException
    {
        retryOnRateLimit(() -> {
            client.shutdown();
            return null; //avoid restrict on generics with void
        });
    }

    /**
     * Retry func if ThrottlingException happen.
     * Retry interval gradually increases with random jitter.
     * @param func
     * @param <T>
     * @return
     * @throws AmazonServiceException
     */
    @VisibleForTesting
    public <T> T retryOnRateLimit(Supplier<T> func) throws AmazonServiceException
    {
        //ToDo  AmazonECSClient has its own retry policy mechanism. Evaluate it and consider it as replacement of this method.
        for (int i = 0; i < rateLimitMaxRetry; i++) {
            try {
                return func.get();
            }
            catch (AmazonServiceException ex) {
                if (RetryUtils.isThrottlingException(ex)) {
                    logger.debug("Rate exceed: {}. Will be retried.", ex.toString());
                    // Max of baseWaitSecs is rateLimitMaxBaseWaitSecs
                    final long baseWaitSecs = Math.min(rateLimitBaseIncrementalSecs * i, rateLimitMaxBaseWaitSecs);
                    waitWithRandomJitter(baseWaitSecs, rateLimitMaxJitterSecs);
                }
                else {
                    throw ex;
                }
            }
        }
        logger.error("Failed to call EcsClient method after Retried {} times", rateLimitMaxRetry);
        throw new RuntimeException("Failed to call EcsClient method");
    }

    @VisibleForTesting
    public void waitWithRandomJitter(long baseWaitSecs, long baseJitterSecs)
    {
        try {
            long jitterSecs = (long) (baseJitterSecs * Math.random());
            Thread.sleep((baseWaitSecs + jitterSecs) * 1000);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}

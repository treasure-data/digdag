package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.AccessDeniedException;
import com.amazonaws.services.ecs.model.BlockedException;
import com.amazonaws.services.ecs.model.ClusterNotFoundException;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
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
import com.amazonaws.services.ecs.model.TagResourceRequest;
import com.amazonaws.services.ecs.model.TagResourceResult;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskSetNotFoundException;
import com.amazonaws.services.ecs.model.UnsupportedFeatureException;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultEcsClient
        implements EcsClient
{
    private static Logger logger = LoggerFactory.getLogger(EcsClient.class);

    private final EcsClientConfig config;
    private final AmazonECSClient client;
    private final AWSLogs logs;

    protected DefaultEcsClient(
            final EcsClientConfig config,
            final AmazonECSClient client,
            final AWSLogs logs)
    {
        this.config = config;
        this.client = client;
        this.logs = logs;
    }

    @Override
    public EcsClientConfig getConfig()
    {
        return config;
    }

    /**
     * Run task on AWS ECS.
     * https://docs.aws.amazon.com/cli/latest/reference/ecs/run-task.html
     *
     * @param request
     * @return
     */
    @Override
    public RunTaskResult submitTask(final RunTaskRequest request)
            throws ConfigException
    {
        try {
            return client.runTask(request);
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

    @Override
    public TaskDefinition getTaskDefinition(final String taskDefinitionArn)
    {
        final DescribeTaskDefinitionRequest request = new DescribeTaskDefinitionRequest()
                .withTaskDefinition(taskDefinitionArn);
        final DescribeTaskDefinitionResult result;
        try {
            result = client.describeTaskDefinition(request);
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
        final ListTagsForResourceResult tagsResult = client.listTagsForResource(tagsRequest); // several runtime exception
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

            final ListTaskDefinitionsResult listResult = client.listTaskDefinitions(listRequest);
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
            result = client.describeTasks(request); // several exceptions
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
        client.stopTask(request);
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
                .withLogGroupName(groupName)
                .withLogStreamName(streamName);
        if (nextToken.isPresent()) {
            request.withNextToken("f/" + nextToken.get());
        }
        return logs.getLogEvents(request);
    }

    @Override
    public void close()
            throws IOException
    {
        client.shutdown();
    }
}

package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import io.digdag.client.config.ConfigException;

import java.io.IOException;
import java.util.List;

public interface EcsClient
        extends AutoCloseable
{
    EcsClientConfig getConfig();

    RunTaskResult submitTask(RunTaskRequest request)
            throws ConfigException;

    TaskDefinition getTaskDefinition(String taskDefinitionArn);

    @FunctionalInterface
    interface FilterFunction
    {
        boolean match(TaskDefinition td);
    }

    List<Tag> getTaskDefinitionTags(final String taskDefinitionArn);

    Optional<TaskDefinition> getTaskDefinitionByTags(List<Tag> tags);

    Optional<TaskDefinition> getTaskDefinitionByTags(Predicate<List<Tag>> tags);

    Task getTask(String cluster, String taskArn);

    void stopTask(String cluster, String taskArn);

    GetLogEventsResult getLog(String groupName, String streamName, Optional<String> nextToken);

    @Override
    void close() throws IOException;
}

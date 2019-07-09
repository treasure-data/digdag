package io.digdag.standards.command;

import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.Task;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class EcsCommandExecutorTest
{

    private Config systemConfig;
    @Mock EcsClientFactory ecsClientFactory;
    @Mock DockerCommandExecutor docker;
    @Mock StorageManager storageManager;
    @Mock ProjectArchiveLoader projectArchiveLoader;
    @Mock CommandLogger clog;

    private EcsCommandExecutor executor;

    @Before
    public void setUp()
    {
        ObjectMapper om = new ObjectMapper().registerModule(new GuavaModule());
        ConfigFactory cf = new ConfigFactory(om);
        this.systemConfig = cf.create();
        executor = spy(new EcsCommandExecutor(systemConfig, ecsClientFactory, docker, storageManager, projectArchiveLoader, clog));
    }

    @Test
    public void testCreateCurrentStatus()
    {
        List<Tag> taskDefinitionTags = ImmutableList.of(
                new Tag().withKey("tag1").withValue("v1"),
                new Tag().withKey("tag2").withValue("v2")
        );
        List<Tag> taskTags = ImmutableList.of(
                new Tag().withKey("tag3").withValue("v3"),
                new Tag().withKey("tag4").withValue("v4")
        );

        CommandContext commandContext = mock(CommandContext.class);
        CommandRequest commandRequest = mock(CommandRequest.class);
        doReturn(new File(".").toPath()).when(commandRequest).getIoDirectory();
        EcsClientConfig ecsClientConfig = mock(EcsClientConfig.class);
        doReturn("my_claster").when(ecsClientConfig).getClusterName();
        Task runTask = mock(Task.class);
        doReturn("my_task_arn").when(runTask).getTaskArn();
        doReturn(taskTags).when(runTask).getTags();
        doReturn(new Date()).when(runTask).getCreatedAt();

        Optional<ObjectNode> awsLogs = Optional.absent();

        final ObjectNode actual = executor.createCurrentStatus(commandContext, commandRequest, ecsClientConfig, taskDefinitionTags, runTask, awsLogs);
        assertEquals("v1", actual.get("task_definition_tags").get("tag1").asText());
        assertEquals("v2", actual.get("task_definition_tags").get("tag2").asText());
        assertEquals("v3", actual.get("task_tags").get("tag3").asText());
        assertEquals("v4", actual.get("task_tags").get("tag4").asText());
    }
}

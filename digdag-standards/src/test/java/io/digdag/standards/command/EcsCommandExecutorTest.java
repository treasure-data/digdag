package io.digdag.standards.command;

import com.amazonaws.services.ecs.model.Container;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskSetNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.ecs.EcsClient;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import io.digdag.standards.command.EcsCommandExecutor.EcsCommandStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EcsCommandExecutorTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory configFactory = new ConfigFactory(om);

    private Config systemConfig;
    @Mock private EcsClientFactory ecsClientFactory;
    @Mock private DockerCommandExecutor dockerCommandExecutor;
    @Mock private StorageManager storageManager;
    @Mock private ProjectArchiveLoader projectArchiveLoader;
    @Mock private CommandLogger commandLogger;

    @Before
    public void setUp()
            throws Exception
    {
        this.systemConfig = configFactory.create();
    }

    @Test
    public void testRun()
            throws Exception
    {
        final EcsCommandExecutor executor = spy(new EcsCommandExecutor(
                systemConfig, ecsClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));

        final EcsCommandExecutor.EcsCommandStatus commandStatus = EcsCommandStatus.of(false, om.createObjectNode().put("foo", "bar"));
        doReturn(mock(EcsClientConfig.class)).when(executor).createEcsClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doReturn(commandStatus).when(executor).run(any(CommandContext.class), any(CommandRequest.class));
        when(ecsClientFactory.createClient(any(EcsClientConfig.class))).thenReturn(mock(EcsClient.class));

        CommandContext commandContext = mock(CommandContext.class);
        CommandRequest commandRequest = mock(CommandRequest.class);
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();

        CommandStatus actual = executor.run(commandContext, commandRequest);
        assertThat(actual.isFinished(), is(commandStatus.isFinished()));
        assertThat(actual.toJson(), is(commandStatus.toJson()));
    }

    @Test
    public void testPoll()
            throws Exception
    {
        final EcsClient ecsClient = mock(EcsClient.class);
        final CommandContext commandContext = mock(CommandContext.class);
        final EcsCommandExecutor executor = spy(new EcsCommandExecutor(
                systemConfig, ecsClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));
        final EcsCommandStatus commandStatus = EcsCommandStatus.of(false, om.createObjectNode().put("foo", "bar"));

        doReturn(mock(EcsClientConfig.class)).when(executor).createEcsClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doReturn(ecsClient).when(ecsClientFactory).createClient(any(EcsClientConfig.class));
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();
        doReturn(commandStatus).when(executor).createNextCommandStatus(any(CommandContext.class), any(EcsClient.class), any(ObjectNode.class));

        ObjectNode previousStatusJson = om.createObjectNode()
                .put("cluster_name", "my_cluster")
                .put("task_arn", "my_task_arn");

        CommandStatus actual = executor.poll(commandContext, previousStatusJson);

        assertThat(actual.isFinished(), is(commandStatus.isFinished()));
        assertThat(actual.toJson(), is(commandStatus.toJson()));
    }

    @Test
    public void testGetErrorMessageFromTask()
    {
        final EcsClient ecsClient = mock(EcsClient.class);
        final Task task = mock(Task.class);
        doReturn(task).when(ecsClient).getTask(any(String.class), any(String.class));

        {
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("No container information"));
        }
        {
            Container container = mock(Container.class);
            doReturn(Arrays.asList(container)).when(task).getContainers();
            doReturn("test test test").when(container).getReason();
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("test test test"));
        }
        {
            doThrow(new TaskSetNotFoundException("No task set found")).when(ecsClient).getTask(any(String.class), any(String.class));
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("No task set found"));
        }
        {
            doThrow(new RuntimeException("Task aborted")).when(ecsClient).getTask(any(String.class), any(String.class));
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("Task aborted"));
        }
    }
}

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    public void testCleanup()
            throws Exception
    {
        final EcsCommandExecutor executor = spy(new EcsCommandExecutor(
                systemConfig, ecsClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));

        EcsClient ecsClient = mock(EcsClient.class);

        doReturn(mock(EcsClientConfig.class)).when(executor).createEcsClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        when(ecsClientFactory.createClient(any(EcsClientConfig.class))).thenReturn(ecsClient);

        Task task = mock(Task.class);
        when(task.getTaskArn()).thenReturn("my_task_arn");
        doReturn(task).when(ecsClient).getTask(any(), any());

        CommandContext commandContext = mock(CommandContext.class);
        TaskRequest taskRequest = mock(TaskRequest.class);
        when(taskRequest.getAttemptId()).thenReturn(Long.valueOf(111));
        when(taskRequest.getTaskId()).thenReturn(Long.valueOf(222));
        doReturn(taskRequest).when(commandContext).getTaskRequest();

        ObjectNode previousStatusJson = om.createObjectNode()
                .put("cluster_name", "my_cluster")
                .put("task_arn", "my_task_arn");
        Config state = configFactory.create().set("commandStatus", previousStatusJson);

        executor.cleanup(commandContext, state);
        verify(ecsClient, times(1)).stopTask(eq("my_cluster"), eq("my_task_arn"));
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
        {  // null or "" in getReason() is ignored.
            Container container1 = mock(Container.class);
            Container container2 = mock(Container.class);
            doReturn(Arrays.asList(container1, container2)).when(task).getContainers();
            doReturn(null).when(container1).getReason();
            doReturn("").when(container2).getReason();
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("No container information"));
        }
        {
            doThrow(new TaskSetNotFoundException("No task set found")).when(ecsClient).getTask(any(String.class), any(String.class));
            Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
            assertThat(msg1.or("test failed"), is("No task set found"));
        }
        {
            doThrow(new RuntimeException("Task aborted")).when(ecsClient).getTask(any(String.class), any(String.class));
            try {
                Optional<String> msg1 = EcsCommandExecutor.getErrorMessageFromTask("my_cluster", "my_task_arn", ecsClient);
                fail("Test failed without RuntimeException");
            }
            catch (RuntimeException re) {
                assertThat(re.getMessage(), is("Task aborted"));
            }
            catch (Exception e) {
                fail("Unexpected Exception happened. " + e.toString());
            }
        }
    }

    @Test
    public void testErrorOnEmptyExitCodeFromContainer()
            throws Exception
    {
        final EcsClient ecsClient = mock(EcsClient.class);
        final CommandContext commandContext = mock(CommandContext.class);
        final EcsCommandExecutor executor = spy(new EcsCommandExecutor(
                systemConfig, ecsClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));
        final Task task = mock(Task.class);
        final Container container = mock(Container.class);

        final ObjectNode previousStatusJson = om.createObjectNode();
        previousStatusJson.put("cluster_name", "my_cluster");
        previousStatusJson.put("task_arn", "my_task_arn");
        previousStatusJson.put("executor_state", om.createObjectNode());
        previousStatusJson.put("awslogs", om.createObjectNode().nullNode());

        doReturn(mock(EcsClientConfig.class)).when(executor).createEcsClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doReturn(ecsClient).when(ecsClientFactory).createClient(any(EcsClientConfig.class));
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();
        doReturn("stopped").when(task).getLastStatus();
        doReturn(null).when(container).getExitCode();
        doReturn(Arrays.asList(container)).when(task).getContainers();
        doReturn(task).when(ecsClient).getTask(previousStatusJson.get("cluster_name").asText(), previousStatusJson.get("task_arn").asText());

        CommandStatus commandStatus = executor.poll(commandContext, previousStatusJson);

        assertThat(commandStatus.getStatusCode(), is(1));
    }
}

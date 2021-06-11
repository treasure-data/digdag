package io.digdag.standards.command;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.stub;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.ecs.EcsClient;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import io.digdag.standards.command.ecs.TemporalProjectArchiveStorage;

import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

/**
 * Uses PowerMockRunner for mocking static methods
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(EcsCommandExecutor.class)
public class EcsCommandExecutorTest2 {
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory configFactory = new ConfigFactory(om);

    private Config systemConfig;
    @Mock
    private EcsClientFactory ecsClientFactory;
    @Mock
    private DockerCommandExecutor dockerCommandExecutor;
    @Mock
    private StorageManager storageManager;
    @Mock
    private ProjectArchiveLoader projectArchiveLoader;
    @Mock
    private CommandLogger commandLogger;

    @Before
    public void setUp() throws Exception {
        this.systemConfig = configFactory.create();
    }

    @Test
    public void testPollOutputFileDoesNotExists() throws Exception {
        final EcsClient ecsClient = mock(EcsClient.class);
        final CommandContext commandContext = mock(CommandContext.class);
        final EcsCommandExecutor executor =
                spy(new EcsCommandExecutor(systemConfig, ecsClientFactory, dockerCommandExecutor,
                    storageManager, projectArchiveLoader, commandLogger));
        final ObjectNode previousExecutorStatus =
                om.createObjectNode().put("logging_finished_at", Instant.now().getEpochSecond());

        doReturn(mock(EcsClientConfig.class)).when(executor)
                                             .createEcsClientConfig(any(Optional.class),
                                                 any(Config.class), any(Config.class));
        doReturn(ecsClient).when(ecsClientFactory).createClient(any(EcsClientConfig.class));
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();
        doReturn(previousExecutorStatus).when(executor)
                                        .fetchLogEvents(any(EcsClient.class), any(ObjectNode.class),
                                            any(ObjectNode.class));
        stub(method(EcsCommandExecutor.class, "getErrorMessageFromTask")).toReturn(
            Optional.absent()); // trick for mockStaticPartial 

        final TemporalProjectArchiveStorage temporalStorage =
                spy(TemporalProjectArchiveStorage.of(storageManager, systemConfig));
        doThrow(StorageFileNotFoundException.class).when(temporalStorage)
                                                   .getContentInputStream(anyString());

        doReturn(temporalStorage).when(executor)
                                 .createTemporalProjectArchiveStorage(any(Config.class));

        ObjectNode previousStatusJson = om.createObjectNode()
                                          .put("cluster_name", "my_cluster")
                                          .put("task_arn", "my_task_arn")
                                          .put("task_finished_at", Instant.now().getEpochSecond())
                                          .put("status_code", 0);

        try {
            executor.poll(commandContext, previousStatusJson);
            fail("should not reach here");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString(
                "archive-output.tar.gz does not exist while ECS_END_OF_TASK_LOG_MARK observed"));
        }
    }

    @Test
    public void testPollNotLoggingFinishedAt() throws Exception {
        final EcsClient ecsClient = mock(EcsClient.class);
        final CommandContext commandContext = mock(CommandContext.class);
        final EcsCommandExecutor executor =
                spy(new EcsCommandExecutor(systemConfig, ecsClientFactory, dockerCommandExecutor,
                    storageManager, projectArchiveLoader, commandLogger));
        final ObjectNode previousExecutorStatus = om.createObjectNode(); //.put("logging_finished_at", Instant.now().getEpochSecond());

        doReturn(mock(EcsClientConfig.class)).when(executor)
                                             .createEcsClientConfig(any(Optional.class),
                                                 any(Config.class), any(Config.class));
        doReturn(ecsClient).when(ecsClientFactory).createClient(any(EcsClientConfig.class));
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();
        doReturn(previousExecutorStatus).when(executor)
                                        .fetchLogEvents(any(EcsClient.class), any(ObjectNode.class),
                                            any(ObjectNode.class));
        stub(method(EcsCommandExecutor.class, "getErrorMessageFromTask")).toReturn(
            Optional.absent()); // trick for mockStaticPartial 

        final TemporalProjectArchiveStorage temporalStorage =
                spy(TemporalProjectArchiveStorage.of(storageManager, systemConfig));
        doThrow(StorageFileNotFoundException.class).when(temporalStorage)
                                                   .getContentInputStream(anyString());

        doReturn(temporalStorage).when(executor)
                                 .createTemporalProjectArchiveStorage(any(Config.class));

        ObjectNode previousStatusJson = om.createObjectNode()
                                          .put("cluster_name", "my_cluster")
                                          .put("task_arn", "my_task_arn")
                                          .put("task_finished_at", Instant.now().getEpochSecond())
                                          .put("status_code", 0);

        executor.setMaxWaitForFetchLogEvents(5);

        try {
            executor.poll(commandContext, previousStatusJson);
            fail("should not reach here");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString(
                "archive-output.tar.gz does not exist while ECS_END_OF_TASK_LOG_MARK observed"));
            assertThat(e.getMessage(), containsString("logging_finished_at=null"));
        }
    }

    @Test
    public void testPollExit1() throws Exception {
        final EcsClient ecsClient = mock(EcsClient.class);
        final CommandContext commandContext = mock(CommandContext.class);
        final EcsCommandExecutor executor =
                spy(new EcsCommandExecutor(systemConfig, ecsClientFactory, dockerCommandExecutor,
                    storageManager, projectArchiveLoader, commandLogger));
        final ObjectNode previousExecutorStatus =
                om.createObjectNode().put("logging_finished_at", Instant.now().getEpochSecond());

        doReturn(mock(EcsClientConfig.class)).when(executor)
                                             .createEcsClientConfig(any(Optional.class),
                                                 any(Config.class), any(Config.class));
        doReturn(ecsClient).when(ecsClientFactory).createClient(any(EcsClientConfig.class));
        doReturn(mock(TaskRequest.class)).when(commandContext).getTaskRequest();
        doReturn(previousExecutorStatus).when(executor)
                                        .fetchLogEvents(any(EcsClient.class), any(ObjectNode.class),
                                            any(ObjectNode.class));
        stub(method(EcsCommandExecutor.class, "getErrorMessageFromTask")).toReturn(
            Optional.absent()); // trick for mockStaticPartial 

        final TemporalProjectArchiveStorage temporalStorage =
                spy(TemporalProjectArchiveStorage.of(storageManager, systemConfig));
        doThrow(StorageFileNotFoundException.class).when(temporalStorage)
                                                   .getContentInputStream(anyString());

        doReturn(temporalStorage).when(executor)
                                 .createTemporalProjectArchiveStorage(any(Config.class));

        ObjectNode previousStatusJson = om.createObjectNode()
                                          .put("cluster_name", "my_cluster")
                                          .put("task_arn", "my_task_arn")
                                          .put("task_finished_at", Instant.now().getEpochSecond())
                                          .put("status_code", 1);

        CommandStatus cmdStatus = executor.poll(commandContext, previousStatusJson);
        assertTrue(cmdStatus.isFinished());
        assertEquals(1, cmdStatus.getStatusCode());
        assertFalse(cmdStatus.toJson().has("retry_on_end_of_task_mark_missing"));
    }
}

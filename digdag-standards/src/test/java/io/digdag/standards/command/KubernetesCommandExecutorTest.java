package io.digdag.standards.command;

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
import io.digdag.standards.command.kubernetes.KubernetesClient;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;
import io.digdag.standards.command.kubernetes.KubernetesClientFactory;
import io.digdag.standards.command.kubernetes.TemporalConfigStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesCommandExecutorTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory configFactory = new ConfigFactory(om);

    private Config systemConfig;
    @Mock private KubernetesClientFactory kubernetesClientFactory;
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
        final KubernetesCommandExecutor executor = spy(new KubernetesCommandExecutor(
                systemConfig, kubernetesClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));
        final KubernetesCommandStatus commandStatus = KubernetesCommandStatus.of(false, om.createObjectNode().put("foo", "bar"));
        doReturn(mock(KubernetesClientConfig.class)).when(executor).createKubernetesClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doReturn(mock(TemporalConfigStorage.class)).when(executor).createTemporalConfigStorage(any(Config.class), any(String.class));
        doReturn(commandStatus).when(executor).runOnKubernetes(any(CommandContext.class), any(CommandRequest.class), any(KubernetesClient.class), any(TemporalConfigStorage.class), any(TemporalConfigStorage.class));
        when(kubernetesClientFactory.newClient(any(KubernetesClientConfig.class))).thenReturn(mock(KubernetesClient.class));

        CommandContext commandContext = mock(CommandContext.class);
        CommandRequest commandRequest = mock(CommandRequest.class);
        when(commandContext.getTaskRequest()).thenReturn(mock(TaskRequest.class));

        CommandStatus actual = executor.run(commandContext, commandRequest);
        assertThat(actual.isFinished(), is(commandStatus.isFinished()));
        assertThat(actual.toJson(), is(commandStatus.toJson()));
    }

    @Test
    public void testPoll()
            throws Exception
    {
        final KubernetesCommandExecutor executor = spy(new KubernetesCommandExecutor(
                systemConfig, kubernetesClientFactory, dockerCommandExecutor,
                storageManager, projectArchiveLoader, commandLogger));
        final KubernetesCommandStatus commandStatus = KubernetesCommandStatus.of(false, om.createObjectNode().put("foo", "bar"));
        doReturn(commandStatus).when(executor).getCommandStatusFromKubernetes(any(CommandContext.class), any(ObjectNode.class), any(KubernetesClient.class), any(TemporalConfigStorage.class));
        doReturn(mock(KubernetesClientConfig.class)).when(executor).createKubernetesClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doReturn(mock(TemporalConfigStorage.class)).when(executor).createTemporalConfigStorage(any(Config.class), any(String.class));
        when(kubernetesClientFactory.newClient(any(KubernetesClientConfig.class))).thenReturn(mock(KubernetesClient.class));

        CommandContext commandContext = mock(CommandContext.class);
        when(commandContext.getTaskRequest()).thenReturn(mock(TaskRequest.class));
        ObjectNode previousStatusJson = om.createObjectNode().put("cluster_name", "my_cluster");

        CommandStatus actual = executor.poll(commandContext, previousStatusJson);
        assertThat(actual.isFinished(), is(commandStatus.isFinished()));
        assertThat(actual.toJson(), is(commandStatus.toJson()));
    }
}

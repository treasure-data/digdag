package io.digdag.standards.command;

import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandLogger;
import io.digdag.standards.command.ecs.EcsClient;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class EcsCommandExecutorTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory configFactory = new ConfigFactory(om);

    private Config systemConfig;
    private EcsCommandExecutor commandExecutor;
    @Mock private EcsClientFactory ecsClientFactory;
    @Mock private EcsClient ecsClient;
    @Mock private DockerCommandExecutor dockerCommandExecutor;
    @Mock private StorageManager storageManager;
    @Mock private ProjectArchiveLoader projectArchiveLoader;
    @Mock private CommandLogger commandLogger;

    @Before
    public void setUp()
            throws Exception
    {
        this.systemConfig = configFactory.create();
        this.commandExecutor = spy(new EcsCommandExecutor(systemConfig, ecsClientFactory, dockerCommandExecutor, storageManager, projectArchiveLoader, commandLogger ));
        doReturn(mock(EcsClientConfig.class)).when(commandExecutor).createEcsClientConfig(any(Optional.class), any(Config.class), any(Config.class));
        doNothing().when(commandExecutor).waitWithRandomJitter(anyLong(), anyLong());
        when(ecsClientFactory.createClient(any(EcsClientConfig.class))).thenReturn(ecsClient);
    }

    @Test
    public void getLogWithRetryTest()
    {
        JsonNode prevStatus = om.createObjectNode().set("awslogs", om.createObjectNode()
                                    .put("awslogs-group", "test-group")
                                    .put("awslogs-stream", "test-stream"));

        final List<Boolean> answerList = new ArrayList<>();
        // Throw exception 3times then succeed.
        Answer<GetLogEventsResult> answer = new Answer<GetLogEventsResult>()
        {
            int count = 0;

            public GetLogEventsResult answer(InvocationOnMock invocation)
            {
                count++;
                if (count > 3) {
                    answerList.add(Boolean.TRUE);
                    return new GetLogEventsResult();
                }
                else {
                    AWSLogsException ex = new AWSLogsException("Rate exceeded");
                    ex.setErrorCode("ThrottlingException");
                    ex.setStatusCode(400);
                    ex.setServiceName("AWSLogs");
                    ex.setRequestId("XXXXX");
                    answerList.add(Boolean.FALSE);
                    throw ex;
                }
            }
        };

        when(ecsClient.getLog(any(), any(), any())).then(answer);
        commandExecutor.getLogWithRetry(ecsClient, prevStatus.deepCopy(), Optional.absent());
        assertThat(answerList.size(), is(4));
        assertThat(answerList.get(3), is(true));
    }
}

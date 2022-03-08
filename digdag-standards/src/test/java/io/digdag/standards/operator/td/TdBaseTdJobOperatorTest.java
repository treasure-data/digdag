package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJobRequest;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.standards.operator.td.TdOperatorTestingUtils.newOperatorFactory;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.stub;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TDOperator.class)
public class TdBaseTdJobOperatorTest
{
    @Mock
    TDClient client;

    @Mock
    TDOperator op;

    @Mock
    TaskRequest taskRequest;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp()
    {
        op.client = client;
        when(taskRequest.getProjectId()).thenReturn(2);
        when(taskRequest.getProjectName()).thenReturn(Optional.absent());
        when(taskRequest.getSessionId()).thenReturn((long) 5);
        when(taskRequest.getAttemptId()).thenReturn((long) 4);
        when(taskRequest.getWorkflowName()).thenReturn("wf");
        when(taskRequest.getTaskName()).thenReturn("t");
    }

    @Test
    public void testCleanup() {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();
        String stmt = "select 1";
        Config config = newConfig()
                .set("database", "testdb")
                .set("query", stmt)
                .set("engine", "presto");

        stub(method(TDOperator.class, "fromConfig", BaseTDClientFactory.class, TDOperator.SystemDefaultConfig.class, java.util.Map.class, Config.class, io.digdag.spi.SecretProvider.class )).toReturn(op);
        BaseTdJobOperator jobOp = (BaseTdJobOperator) newOperatorFactory(TdOperatorFactory.class)
                .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));
        jobOp.state.params().set("job", TDOperator.JobState.empty().withJobId("1"));

        jobOp.cleanup(newTaskRequest().withConfig(newConfig().set("job", testTDJobRequestParams(projectPath, config))));

        verify(client, times(1)).killJob("1");
    }


    private TDJobRequest testTDJobRequestParams(Path projectPath, Config config)
    {
        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        ArgumentCaptor<TDJobRequest> captor = ArgumentCaptor.forClass(TDJobRequest.class);
        BaseTdJobOperator operator =
                (BaseTdJobOperator)newOperatorFactory(TdOperatorFactory.class)
                        .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));

        operator.startJob(op, "");

        Mockito.verify(op).submitNewJobWithRetry(captor.capture());
        TDJobRequest jobRequest = captor.getValue();
        return jobRequest;

    }
}

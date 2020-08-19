package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.treasuredata.client.model.TDJobRequest;
import io.digdag.client.config.Config;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.standards.operator.td.TdOperatorTestingUtils.newOperatorFactory;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TdForEachOperatorFactoryTest
{
    @Mock
    TDOperator op;

    /**
     * Check config parameters are set to TDJobRequest
     */
    @Test
    public void testTDJobRequestParams()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
                .set("database", "testdb")
                .set("query", "select 1")
                .set("engine", "presto")
                .set("_do", newConfig().set("+show", newConfig().set("echo>", "aaaa")));

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertEquals("select 1", jobRequest.getQuery());
        assertEquals("presto", jobRequest.getType().toString());
        assertEquals(Optional.absent(), jobRequest.getEngineVersion());

    }

    /**
     * Check config parameters are set to TDJobRequest with engine_version
     */
    @Test
    public void testTDJobRequestParamsWithEngineVersion()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
                .set("database", "testdb")
                .set("query", "select 1")
                .set("engine", "hive")
                .set("engine_version", "stable")
                .set("_do", newConfig().set("+show", newConfig().set("echo>", "aaaa")));

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertEquals("select 1", jobRequest.getQuery());
        assertEquals("hive", jobRequest.getType().toString());
        assertTrue(jobRequest.getEngineVersion().isPresent());
        assertEquals("stable", jobRequest.getEngineVersion().get().toString());
    }

    @Test
    public void testTDJobRequestParamsWithEngineVersionForPrestoIsIgnored()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
                .set("database", "testdb")
                .set("query", "select 1")
                .set("engine", "presto")
                .set("engine_version", "stable")
                .set("_do", newConfig().set("+show", newConfig().set("echo>", "aaaa")));

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertEquals("select 1", jobRequest.getQuery());
        assertEquals("presto", jobRequest.getType().toString());
        assertFalse(jobRequest.getEngineVersion().isPresent());
    }

    private TDJobRequest testTDJobRequestParams(Path projectPath, Config config)
    {
        ArgumentCaptor<TDJobRequest> captor = ArgumentCaptor.forClass(TDJobRequest.class);

        BaseTdJobOperator operator =
                (BaseTdJobOperator) newOperatorFactory(TdForEachOperatorFactory.class)
                        .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));

        operator.startJob(op, "");

        Mockito.verify(op).submitNewJobWithRetry(captor.capture());
        TDJobRequest jobRequest = captor.getValue();
        return jobRequest;
    }
}

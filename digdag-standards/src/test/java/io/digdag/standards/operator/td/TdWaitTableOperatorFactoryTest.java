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

import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.standards.operator.td.TdOperatorTestingUtils.newOperatorFactory;
import static io.digdag.client.config.ConfigUtils.newConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TdWaitTableOperatorFactoryTest
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
                .set("_command", "target_table_00")
                .set("database", "testdb")
                .set("engine", "presto");

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertTrue(jobRequest.getQuery().length() > 0);
        assertEquals("presto", jobRequest.getType().toString());
        assertEquals(Optional.absent(), jobRequest.getEngineVersion());
    }

    /**
     * Check config parameters are set to TDJobRequest
     */
    @Test
    public void testTDJobRequestParamsWithEngineVersionForPrestoIsIgnored()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
                .set("_command", "target_table_00")
                .set("database", "testdb")
                .set("engine", "presto")
                .set("engine_version", "stable");

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertTrue(jobRequest.getQuery().length() > 0);
        assertEquals("presto", jobRequest.getType().toString());
        assertFalse(jobRequest.getEngineVersion().isPresent());
    }

    @Test
    public void testTDJobRequestParamsWithEngineVersion()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
                .set("_command", "target_table_00")
                .set("database", "testdb")
                .set("engine", "hive")
                .set("engine_version", "stable");

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertTrue(jobRequest.getQuery().length() > 0);
        assertEquals("hive", jobRequest.getType().toString());
        assertTrue(jobRequest.getEngineVersion().isPresent());
        assertEquals("stable", jobRequest.getEngineVersion().get().toString());
    }

    private TDJobRequest testTDJobRequestParams(Path projectPath, Config config)
    {
        ArgumentCaptor<TDJobRequest> captor = ArgumentCaptor.forClass(TDJobRequest.class);

        TdWaitTableOperatorFactory.TdWaitTableOperator operator =
                (TdWaitTableOperatorFactory.TdWaitTableOperator) newOperatorFactory(TdWaitTableOperatorFactory.class)
                        .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));

        operator.startJob(op, "");

        Mockito.verify(op).submitNewJobWithRetry(captor.capture());
        TDJobRequest jobRequest = captor.getValue();
        return jobRequest;
    }
}

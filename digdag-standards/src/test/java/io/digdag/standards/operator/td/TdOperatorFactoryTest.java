package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.treasuredata.client.model.TDJobRequest;
import io.digdag.core.EnvironmentModule;
import io.digdag.standards.td.TdConfigurationModule;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import io.digdag.client.config.Config;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static io.digdag.standards.operator.td.TdOperatorFactory.insertCommandStatement;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.standards.operator.td.TdOperatorTestingUtils.newOperatorFactory;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TdOperatorFactoryTest
{
    @Mock
    TDOperator op;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     *
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
                .set("engine", "presto");

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        ArgumentCaptor<TDJobRequest> captor = ArgumentCaptor.forClass(TDJobRequest.class);

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertEquals("select 1", jobRequest.getQuery());
        assertEquals("presto", jobRequest.getType().toString() );
        assertEquals(Optional.absent(), jobRequest.getEngineVersion());
    }

    /**
     *
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
                .set("engine_version", "stable");

        when(op.submitNewJobWithRetry(any(TDJobRequest.class))).thenReturn("");
        when(op.getDatabase()).thenReturn("testdb");

        TDJobRequest jobRequest = testTDJobRequestParams(projectPath, config);

        assertEquals("testdb", jobRequest.getDatabase());
        assertEquals("select 1", jobRequest.getQuery());
        assertEquals("hive", jobRequest.getType().toString() );
        assertTrue(jobRequest.getEngineVersion().isPresent());
        assertEquals("stable", jobRequest.getEngineVersion().get().toString());
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

    @Test
    public void verifyCommandInserts()
    {
        assertEquals(
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "select 1"));

        assertEquals(
                "with a as (select 1)\n" +
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n" +
                    "-- DIGDAG_INSERT_LINE\n" +
                    "select 1"));

        assertEquals(
                "with a as (select 1)\n" +
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n" +
                    "-- DIGDAG_INSERT_LINE xyz\n" +
                    "select 1"));

        assertEquals(
                "with a as (select 1)\n" +
                "INSERT\n" +
                "-- comment\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n" +
                    "--DIGDAG_INSERT_LINE\n" +
                    "-- comment\n" +
                    "select 1"));

        assertEquals(
                "-- comment\n" +
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "-- comment\n" +
                    "select 1"));

        assertEquals(
                "INSERT\n" +
                "select 1\n" +
                "-- comment\n" +
                "from table",
                insertCommandStatement("INSERT",
                    "select 1\n" +
                    "-- comment\n" +
                    "from table"));

        assertEquals(
                "-- comment\r\n" +
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "-- comment\r\n" +
                    "select 1"));

        assertEquals(
                "-- comment1\n" +
                "--comment2\n" +
                "INSERT\n" +
                "select 1",
                insertCommandStatement("INSERT",
                    "-- comment1\n" +
                    "--comment2\n" +
                    "select 1"));

        {
            String command = "INSERT";
            String query = "SELECT\n" +
                    "-- comment1\n" +
                    "1;\n" +
                    "-- comment2\n";
            String expected = command + "\n" + query;
            assertThat(insertCommandStatement(command, query), is(expected));
        }
    }

    @Test
    public void rejectQueryFileOutsideOfProjectPath()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
            .set("_command", projectPath.resolve("..").resolve("parent.sql").toString());

        exception.expectMessage("File name must not be outside of project path");

        newOperatorFactory(TdOperatorFactory.class)
            .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));
    }

    @Test
    public void rejectResultSettingsWithoutResultConnection()
            throws Exception
    {
        Path projectPath = Paths.get("").normalize().toAbsolutePath();

        Config config = newConfig()
            .set("database", "testdb")
            .set("query", "select 1")
            .set("result_settings", "{\"type\":\"http\"}");

        exception.expectMessage("result_settings is valid only if result_connection is set");

        newOperatorFactory(TdOperatorFactory.class)
            .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)));
    }
}

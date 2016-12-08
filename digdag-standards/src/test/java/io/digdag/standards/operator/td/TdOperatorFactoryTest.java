package io.digdag.standards.operator.td;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;

import static io.digdag.standards.operator.td.TdOperatorFactory.insertCommandStatement;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;

public class TdOperatorFactoryTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

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
            .newOperator(newContext(projectPath, newTaskRequest().withConfig(config)))
            .close();
    }
}

package io.digdag.standards.operator.td;

import org.junit.Test;

import static io.digdag.standards.operator.td.TdOperatorFactory.insertCommandStatement;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TdOperatorFactoryTest
{
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
}

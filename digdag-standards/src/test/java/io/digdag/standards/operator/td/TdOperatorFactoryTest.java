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
        assertEquals("INSERT\nselect 1",
                insertCommandStatement("INSERT",
                    "select 1"));

        assertEquals("with a as (select 1)\nINSERT\nselect 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n-- DIGDAG_INSERT_LINE\nselect 1"));

        assertEquals("with a as (select 1)\nINSERT\nselect 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n-- DIGDAG_INSERT_LINE xyz\nselect 1"));

        assertEquals("with a as (select 1)\nINSERT\n-- comment\nselect 1",
                insertCommandStatement("INSERT",
                    "with a as (select 1)\n--DIGDAG_INSERT_LINE\n-- comment\nselect 1"));

        assertEquals("-- comment\nINSERT\nselect 1",
                insertCommandStatement("INSERT",
                    "-- comment\nselect 1"));

        assertEquals("-- comment\nINSERT\r\nselect 1",
                insertCommandStatement("INSERT",
                    "-- comment\r\nselect 1"));

        assertEquals("-- comment1\n--comment2\nINSERT\nselect 1",
                insertCommandStatement("INSERT",
                    "-- comment1\n--comment2\nselect 1"));

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

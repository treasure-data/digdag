package io.digdag.standards.operator.pg;

import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.pg.PgOperatorFactory.PgOperator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.ImmutableMapValue;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PgOperatorTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final PgOpTestHelper testHelper = new PgOpTestHelper();

    @Test
    public void test()
            throws IOException
    {
        TaskRequest taskRequest = testHelper.createTaskRequest(ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "query", "SELECT * FROM users"
        ));
        PgOperator operator = (PgOperator) testHelper.injector().getInstance(PgOperatorFactory.class).newTaskExecutor(testHelper.workpath(), taskRequest);
        exception.expect(TaskExecutionException.class);
        operator.runTask();
    }
}
package io.digdag.standards.operator.pg;

import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class PgOperatorFactoryTest
{
    private PgOpTestHelper testHelper = new PgOpTestHelper();
    private PgOperatorFactory operatorFactory;

    @Before
    public void setUp()
    {
        operatorFactory = testHelper.injector().getInstance(PgOperatorFactory.class);
    }

    @Test
    public void getKey()
    {
        assertThat(operatorFactory.getType(), is("pg"));
    }

    @Test
    public void newTaskExecutor()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar0.org",
                "user", "user0",
                "database", "database0"
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput);
        Operator operator = operatorFactory.newTaskExecutor(testHelper.workpath(), taskRequest);
        assertThat(operator, is(instanceOf(PgOperatorFactory.PgOperator.class)));
    }
}
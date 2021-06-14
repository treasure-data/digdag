package io.digdag.standards.operator.pg;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.operator.jdbc.JdbcOpTestHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PgOperatorFactoryTest
{
    private JdbcOpTestHelper testHelper = new JdbcOpTestHelper();
    private PgOperatorFactory operatorFactory;

    @Before
    public void setUp()
    {
        operatorFactory = newOperatorFactory(PgOperatorFactory.class);
    }

    @Test
    public void getKey()
    {
        assertThat(operatorFactory.getType(), is("pg"));
    }

    @Test
    public void newOperator()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar0.org",
                "user", "user0",
                "database", "database0"
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        Operator operator = operatorFactory.newOperator(newContext(testHelper.projectPath(), taskRequest));
        assertThat(operator, is(instanceOf(PgOperatorFactory.PgOperator.class)));
    }
}

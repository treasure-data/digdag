package io.digdag.standards.operator.pg;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.Operator;
import io.digdag.spi.TemplateEngine;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class PgOperatorFactoryTest
{
    private PgOperatorFactory operatorFactory;

    static class MyModule implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(TemplateEngine.class).to(ConfigEvalEngine.class).in(Scopes.SINGLETON);
        }
    }

    @Before
    public void setUp()
    {
        Injector injector = Guice.createInjector(new MyModule());
        operatorFactory = injector.getInstance(PgOperatorFactory.class);
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
        ConfigBuilder configBuilder = new ConfigBuilder();
        Map<String, String> configInput = ImmutableMap.of(
                "host", "foobar0.org",
                "user", "user0",
                "database", "database0"
        );
        Config config = configBuilder.createConfig(configInput);

        ImmutableTaskRequest taskRequest = ImmutableTaskRequest.
                builder().
                siteId(1).
                projectId(2).
                workflowName("wf").
                taskId(3).
                attemptId(4).
                sessionId(5).
                taskName("t").
                queueName("q").
                lockId("l").
                priority(6).
                timeZone(ZoneId.systemDefault()).
                sessionUuid(UUID.randomUUID()).
                sessionTime(Instant.now()).
                createdAt(Instant.now()).
                config(config).
                localConfig(configBuilder.createConfig(ImmutableMap.of())).
                lastStateParams(configBuilder.createConfig(ImmutableMap.of())).
                build();
        Operator operator = operatorFactory.newTaskExecutor(Files.createTempDirectory("pg_op_test"), taskRequest);
        assertThat(operator, is(instanceOf(PgOperatorFactory.PgOperator.class)));
    }
}
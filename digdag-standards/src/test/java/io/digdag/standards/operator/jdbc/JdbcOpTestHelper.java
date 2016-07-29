package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

public class JdbcOpTestHelper
{
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new GuavaModule());
    private final YamlConfigLoader loader = new YamlConfigLoader();
    private final ConfigFactory configFactory = new ConfigFactory(mapper);
    private final Injector injector = Guice.createInjector(new MyModule());

    static class MyModule implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(TemplateEngine.class).to(ConfigEvalEngine.class).in(Scopes.SINGLETON);
        }
    }

    public Injector injector()
    {
        return injector;
    }

    public Path workpath()
            throws IOException
    {
        return Files.createTempDirectory("jdbc_op_test");
    }

    public Config createConfig(Map<String, Object> configInput)
            throws IOException
    {
        return loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);
    }

    public TaskRequest createTaskRequest(Map<String, Object> configInput, Optional<Map<String, Object>> lastState)
            throws IOException
    {
        Config config = createConfig(configInput);
        return ImmutableTaskRequest.
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
                localConfig(createConfig(ImmutableMap.of())).
                lastStateParams(createConfig(lastState.or(ImmutableMap.of()))).
                build();
    }

    public ConfigFactory getConfigFactory()
    {
        return configFactory;
    }
}

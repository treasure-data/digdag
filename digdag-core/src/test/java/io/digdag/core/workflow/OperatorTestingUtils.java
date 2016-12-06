package io.digdag.core.workflow;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.Environment;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.GrantedPrivilegedVariables;
import io.digdag.core.agent.OperatorRegistry;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TemplateEngine;

import java.util.Properties;
import java.util.Map;
import java.util.UUID;
import java.time.Instant;
import java.time.ZoneId;

import static io.digdag.client.config.ConfigUtils.newConfig;

public class OperatorTestingUtils
{
    private OperatorTestingUtils()
    { }

    public static <T extends OperatorFactory> T newOperatorFactory(Class<T> factoryClass)
    {
        Injector initInjector = Guice.createInjector((Module) (binder) -> {
            binder.bind(CommandExecutor.class).to(SimpleCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(TemplateEngine.class).to(ConfigEvalEngine.class).in(Scopes.SINGLETON);
            binder.bind(ConfigFactory.class).toInstance(ConfigUtils.configFactory);
            binder.bind(Config.class).toInstance(newConfig());
            binder.bind(OperatorRegistry.DynamicOperatorPluginInjectionModule.class).in(Scopes.SINGLETON);
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Environment.class).toInstance(ImmutableMap.<String, String>of());
        });

        Injector injector = Guice.createInjector(
                (Module) initInjector.getInstance(OperatorRegistry.DynamicOperatorPluginInjectionModule.class),
                (Module) (binder) -> binder.bind(factoryClass)
                );

        return injector.getInstance(factoryClass);
    }

    public static ImmutableTaskRequest newTaskRequest()
    {
        return ImmutableTaskRequest.builder()
            .siteId(1)
            .projectId(2)
            .workflowName("wf")
            .taskId(3)
            .attemptId(4)
            .sessionId(5)
            .taskName("t")
            .lockId("l")
            .timeZone(ZoneId.systemDefault())
            .sessionUuid(UUID.randomUUID())
            .sessionTime(Instant.now())
            .createdAt(Instant.now())
            .config(newConfig())
            .localConfig(newConfig())
            .lastStateParams(newConfig())
            .build();
    }

    public static TestingTaskExecutionContext newContext()
    {
        return new TestingTaskExecutionContext(
                GrantedPrivilegedVariables.empty(),
                TestingSecretProvider.empty());
    }

    public static class TestingTaskExecutionContext
            implements TaskExecutionContext
    {
        private final PrivilegedVariables privilegedVariables;
        private final SecretProvider secrets;

        public TestingTaskExecutionContext(PrivilegedVariables privilegedVariables, SecretProvider secrets)
        {
            this.privilegedVariables = privilegedVariables;
            this.secrets = secrets;
        }

        @Override
        public PrivilegedVariables privilegedVariables()
        {
            return privilegedVariables;
        }

        @Override
        public SecretProvider secrets()
        {
            return secrets;
        }

        public TestingTaskExecutionContext withSecrets(Properties secretsProps)
        {
            return new TestingTaskExecutionContext(
                    privilegedVariables,
                    TestingSecretProvider.fromProperties(secretsProps));
        }

        public TestingTaskExecutionContext withPrivilegedVariables(Config grants, Config params)
        {
            return new TestingTaskExecutionContext(
                    GrantedPrivilegedVariables.build(grants, params, secrets),
                    secrets);
        }
    }

    private static class TestingSecretProvider
            implements SecretProvider
    {
        public static TestingSecretProvider empty()
        {
            return fromProperties(new Properties());
        }

        public static TestingSecretProvider fromProperties(Properties props)
        {
            return new TestingSecretProvider(props);
        }

        private final Properties props;

        private TestingSecretProvider(Properties props)
        {
            this.props = props;
        }

        public Optional<String> getSecretOptional(String key)
        {
            return Optional.fromNullable(props.getProperty(key));
        }
    }
}

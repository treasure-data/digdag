package io.digdag.core.agent;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.stream.Collectors;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.TemplateEngine;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Environment;
import io.digdag.core.plugin.PluginSet;
import io.digdag.core.plugin.PluginLoader;
import io.digdag.core.plugin.DynamicPluginLoader;
import io.digdag.core.plugin.Spec;

public class OperatorRegistry
{
    public static class DynamicOperatorPluginInjectionModule
            implements Module
    {
        @Inject
        protected CommandExecutor commandExecutor;

        @Inject
        protected CommandLogger commandLogger;

        @Inject
        protected TemplateEngine templateEngine;

        @Inject
        protected ConfigFactory cf;

        @Inject
        protected Config systemConfig;

        @Inject
        @Environment
        protected Map<String, String> environment;

        @Override
        public void configure(Binder binder)
        {
            binder.bind(CommandExecutor.class).toInstance(commandExecutor);
            binder.bind(CommandLogger.class).toInstance(commandLogger);
            binder.bind(TemplateEngine.class).toInstance(templateEngine);
            binder.bind(ConfigFactory.class).toInstance(cf);
            binder.bind(Config.class).toInstance(systemConfig);
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Environment.class).toInstance(environment);
        }
    }

    private final Map<String, OperatorFactory> map;
    private final DynamicPluginLoader<Map<String, OperatorFactory>> dynamicLoader;

    @Inject
    public OperatorRegistry(
            Set<OperatorFactory> injectedOperators,
            PluginSet.WithInjector systemPlugins,
            PluginLoader dynamicPluginLoader,
            DynamicOperatorPluginInjectionModule dynamicLoaderModule)
    {
        // built-in operators are
        // the operators loaded by Extension interface (injectedOperators)
        // and operators loaded by Plugin interface (systemPlugins).
        ImmutableMap.Builder<String, OperatorFactory> builder = ImmutableMap.builder();
        builder.putAll(buildTypeMap(loadOperatorFactories(systemPlugins)));
        builder.putAll(buildTypeMap(injectedOperators));  // extension operators have higher priority
        this.map = builder.build();

        this.dynamicLoader = DynamicPluginLoader.build(
                dynamicPluginLoader,
                dynamicLoaderModule,
                plugins -> buildTypeMap(loadOperatorFactories(plugins)),
                10);
    }

    public OperatorFactory get(TaskRequest request, String type)
    {
        // built-in operators have higher priority
        OperatorFactory factory = map.get(type);
        if (factory != null) {
            return factory;
        }

        return dynamicLoader.load(getSpec(request)).get(type);
    }

    private static Spec getSpec(TaskRequest request)
    {
        Config params = request.getConfig()
            .mergeDefault(request.getConfig().getNestedOrGetEmpty("plugin"));
        List<String> repositories = params.getListOrEmpty("repositories", String.class);
        List<String> dependencies = params.getListOrEmpty("dependencies", String.class);
        return Spec.of(repositories, dependencies);
    }

    private static List<OperatorFactory> loadOperatorFactories(PluginSet.WithInjector plugins)
    {
        return plugins.getServiceProviders(OperatorProvider.class)
            .stream()
            .flatMap(operatorProvider -> operatorProvider.get().stream())
            .collect(Collectors.toList());
    }

    private static Map<String, OperatorFactory> buildTypeMap(Collection<OperatorFactory> factories)
    {
        ImmutableMap.Builder<String, OperatorFactory> builder = ImmutableMap.builder();

        for (OperatorFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }

        return builder.build();
    }
}

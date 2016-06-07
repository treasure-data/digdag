package io.digdag.core.agent;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.core.plugin.PluginSet;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import io.digdag.core.plugin.DynamicPluginLoader;
import io.digdag.core.plugin.Spec;

public class OperatorRegistry
{
    private final Map<String, OperatorFactory> map;
    private final DynamicPluginLoader<Map<String, OperatorFactory>> dynamicLoader;

    @Inject
    public OperatorRegistry(
            Set<OperatorFactory> injectedOperators,
            PluginSet systemPlugins,
            DynamicPluginLoader.Builder dynamicLoaderBuilder)
    {
        ImmutableMap.Builder<String, OperatorFactory> builder = ImmutableMap.builder();

        // load operators from plugins
        builder.putAll(buildTypeMap(systemPlugins.get(OperatorFactory.class)));

        // get operators that are already loaded by Extension
        // injectedOperators have higher priority
        builder.putAll(buildTypeMap(injectedOperators));

        this.map = builder.build();
        this.dynamicLoader = dynamicLoaderBuilder.build(10, plugins -> buildTypeMap(plugins.get(OperatorFactory.class)));
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
        List<String> repositories = ImmutableList.of();
        List<String> dependencies = params.getList("dependencies", String.class);
        return Spec.of(repositories, dependencies);
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

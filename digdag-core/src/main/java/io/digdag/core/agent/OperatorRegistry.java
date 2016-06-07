package io.digdag.core.agent;

import java.util.Set;
import java.util.Map;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableMap;
import io.digdag.core.plugin.PluginSet;
import io.digdag.spi.OperatorFactory;

public class OperatorRegistry
{
    private final Map<String, OperatorFactory> map;

    @Inject
    public OperatorRegistry(
            Set<OperatorFactory> injectedOperators,
            PluginSet systemPlugins)
    {
        ImmutableMap.Builder<String, OperatorFactory> builder = ImmutableMap.builder();

        for (OperatorFactory factory : systemPlugins.get(OperatorFactory.class)) {
            builder.put(factory.getType(), factory);
        }

        // injectedOperators have higher priority
        for (OperatorFactory factory : injectedOperators) {
            builder.put(factory.getType(), factory);
        }

        this.map = builder.build();
    }

    public OperatorFactory get(String type)
    {
        return map.get(type);
    }
}

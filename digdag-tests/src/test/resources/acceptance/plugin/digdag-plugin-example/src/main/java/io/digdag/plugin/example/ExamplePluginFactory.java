package io.digdag.plugin.example;

import java.util.List;
import com.google.inject.Injector;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.Plugin;
import io.digdag.spi.PluginFactory;
import io.digdag.spi.OperatorFactory;

public class ExamplePluginFactory
        implements PluginFactory
{
    @Override
    public Plugin create(Injector injector)
    {
        return new ExamplePlugin(injector);
    }

    public static class ExamplePlugin
            implements Plugin
    {
        private final Injector injector;

        public ExamplePlugin(Injector injector)
        {
            this.injector = injector
                .createChildInjector((binder) -> {
                    binder.bind(ExampleOperatorFactory.class);
                });
        }

        @Override
        public <T> List<T> get(Class<T> iface)
        {
            if (iface == OperatorFactory.class) {
                return ImmutableList.of(
                        iface.cast(injector.getInstance(ExampleOperatorFactory.class)));
            }
            else {
                return ImmutableList.of();
            }
        }
    }
}

package io.digdag.plugin.example;

import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import io.digdag.spi.Plugin;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.TemplateEngine;

public class ExamplePlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == OperatorProvider.class) {
            return ExampleOperatorProvider.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    public static class ExampleOperatorProvider
            implements OperatorProvider
    {
        @Inject
        protected TemplateEngine templateEngine;

        @Override
        public List<OperatorFactory> get()
        {
            return Arrays.asList(
                    new ExampleOperatorFactory(templateEngine));
        }
    }
}

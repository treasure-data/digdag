package io.digdag.standards.operator.td;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.Environment;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.OperatorRegistry;
import io.digdag.core.agent.TaskContextCommandLogger;
import io.digdag.core.workflow.MockCommandExecutor;
import io.digdag.core.workflow.OperatorTestingUtils;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.td.TdConfigurationModule;

import java.util.Map;

import static io.digdag.client.config.ConfigUtils.newConfig;

public class TdOperatorTestingUtils

{
    public static <T extends OperatorFactory> T newOperatorFactory(Class<T> factoryClass) {
        Injector initInjector = Guice.createInjector((Module) (binder) -> {
            binder.bind(CommandExecutor.class).to(MockCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(CommandLogger.class).to(TaskContextCommandLogger.class).in(Scopes.SINGLETON);
            binder.bind(TemplateEngine.class).to(ConfigEvalEngine.class).in(Scopes.SINGLETON);
            binder.bind(ConfigFactory.class).toInstance(ConfigUtils.configFactory);
            binder.bind(Config.class).toInstance(newConfig());
            binder.bind(OperatorRegistry.DynamicOperatorPluginInjectionModule.class).in(Scopes.SINGLETON);
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Environment.class).toInstance(ImmutableMap.<String, String>of());
        });

        Injector injector = Guice.createInjector(
                (Module) initInjector.getInstance(OperatorRegistry.DynamicOperatorPluginInjectionModule.class),
                (Module) (binder) -> binder.bind(factoryClass),
                new TdConfigurationModule()
        );

        return injector.getInstance(factoryClass);

    }

}

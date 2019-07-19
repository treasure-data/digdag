package io.digdag.server.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.metrics.DigdagMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import java.util.Map;
import java.util.stream.Collectors;


public class DigdagMetricsModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        configureMeterRegistry();
        binder().bind(DigdagMetrics.class).to(StdDigdagMetrics.class);
        configureInterceptor();
    }

    /**
     *  Configure MeterRegistry.
     *  As default, JmxMeterRegistry is used.
     *  If want to change MeterRegistry, override this method.
     */
    public void configureMeterRegistry()
    {
        bindMeterRegistry("default");
        bindMeterRegistry("api");
        bindMeterRegistry("agent");
        bindMeterRegistry("executor");
        bindMeterRegistry("db");
    }

    protected void bindMeterRegistry(String category)
    {
        binder().bind(MeterRegistry.class)
                .annotatedWith(Names.named(category))
                .toInstance(createCompositeMeterRegistry(category));
    }

    /**
     * Override this method is easy way to add additional MeterRegistry
     * @param category
     * @return
     */
    protected CompositeMeterRegistry createCompositeMeterRegistry(String category)
    {
        return new CompositeMeterRegistry().add(createJmxMeterRegistry(category));
    }


    private final Map<String,String> categoryToJMXdomain = ImmutableMap.of(
            "default", "io.digdag",
            "api", "io.digdag.api",
            "agent", "io.digdag.agent",
            "executor", "io.digdag.executor",
            "db", "io.digdag.db"
    );

    private JmxMeterRegistry createJmxMeterRegistry(String category)
    {
        return new DigdagJmxMeterRegistry(createJmxConfig(categoryToJMXdomain.get(category)), Clock.SYSTEM);
    }

    private JmxConfig createJmxConfig(String domain)
    {
        return new JmxConfig() {
            @Override
            public String get(String key) {
                return null;
            }
            @Override
            public String domain() {
                return domain;
            }

        };
    }

    // Set interceptor for @DigdagTimed annotation
    public void configureInterceptor()
    {
        DigdagTimedMethodInterceptor interceptor = new DigdagTimedMethodInterceptor();
        requestInjection(interceptor);
        binder().bindInterceptor(Matchers.any(), Matchers.annotatedWith(DigdagTimed.class), interceptor);
    }
}

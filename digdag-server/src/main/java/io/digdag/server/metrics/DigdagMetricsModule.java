package io.digdag.server.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.metrics.DigdagTimed;
import io.digdag.server.metrics.jmx.DigdagJmxMeterRegistry;
import io.digdag.spi.metrics.DigdagMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import static io.digdag.spi.metrics.DigdagMetrics.Category;


public class DigdagMetricsModule
        extends AbstractModule
{
    private DigdagMetricsConfig metricsConfig;

    private static final Logger logger = LoggerFactory.getLogger(DigdagMetricsModule.class);

    public DigdagMetricsModule(DigdagMetricsConfig metricsConfig)
    {
        this.metricsConfig = metricsConfig;
    }

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
        bindMeterRegistry(Category.DEFAULT);
        bindMeterRegistry(Category.AGENT);
        bindMeterRegistry(Category.API);
        bindMeterRegistry(Category.DB);
        bindMeterRegistry(Category.EXECUTOR);
    }

    protected void bindMeterRegistry(Category category)
    {
        binder().bind(MeterRegistry.class)
                .annotatedWith(Names.named(category.getString()))
                .toInstance(createCompositeMeterRegistry(category));
    }

    /**
     * Override this method is easy way to add additional MeterRegistry
     * @param category
     * @return
     */
    protected CompositeMeterRegistry createCompositeMeterRegistry(Category category)
    {
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        if (metricsConfig.getJmxPluginConfig().enable(category)) {
            registry.add(createJmxMeterRegistry(category));
        }
        return registry;
    }


    private final Map<Category,String> categoryToJMXdomain = ImmutableMap.of(
            Category.DEFAULT, "io.digdag",
            Category.AGENT, "io.digdag.agent",
            Category.API, "io.digdag.api",
            Category.DB, "io.digdag.db",
            Category.EXECUTOR, "io.digdag.executor"
    );

    private JmxMeterRegistry createJmxMeterRegistry(Category category)
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

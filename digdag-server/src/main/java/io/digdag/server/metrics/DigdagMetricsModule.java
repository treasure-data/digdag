package io.digdag.server.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import io.digdag.client.config.ConfigException;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.metrics.DigdagTimed;
import io.digdag.server.metrics.fluency.FluencyMonitorSystemConfig;
import io.digdag.spi.metrics.DigdagMetrics;
import io.github.yoyama.micrometer.FluencyMeterRegistry;
import io.github.yoyama.micrometer.FluencyRegistryConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import static io.digdag.spi.metrics.DigdagMetrics.Category;


public class DigdagMetricsModule
        extends AbstractModule
{
    // To customize easily
    protected final DigdagMetricsConfig metricsConfig;

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

        if (isEnableCategory("jmx", category)) {
            registry.add(createJmxMeterRegistry(category));
        }
        if (isEnableCategory("fluency", category)) {
            registry.add(createFluencyMeterRegistry(category));
        }

        return registry;
    }

    protected boolean isEnableCategory(String key, Category category)
    {
        return metricsConfig.getMonitorSystemConfig(key).transform( (p) -> p.getCategoryEnable(category)).or(false);
    }


    private final Map<Category,String> categoryToJMXdomain = ImmutableMap.of(
            Category.DEFAULT, "io.digdag",
            Category.AGENT, "io.digdag.agent",
            Category.API, "io.digdag.api",
            Category.DB, "io.digdag.db",
            Category.EXECUTOR, "io.digdag.executor"
    );

    // To customize easily
    protected JmxMeterRegistry createJmxMeterRegistry(Category category)
    {
        return new JmxMeterRegistry(createJmxConfig(categoryToJMXdomain.get(category)), Clock.SYSTEM);
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

    @VisibleForTesting
    public FluencyMeterRegistry createFluencyMeterRegistry(Category category)
    {
        FluencyMonitorSystemConfig fconfig = (FluencyMonitorSystemConfig)metricsConfig
                .getMonitorSystemConfig("fluency")
                .or(() -> {throw new ConfigException("fluency is disabled");});
        Fluency fluency = FluencyMonitorSystemConfig.createFluency(fconfig);
        FluencyRegistryConfig regConfig = FluencyRegistryConfig.apply(fconfig.getTag(), "digdag", Duration.ofSeconds(fconfig.getStep()), false);
        return FluencyMeterRegistry.apply(regConfig, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM, fluency);
    }


    // Set interceptor for @DigdagTimed annotation
    public void configureInterceptor()
    {
        DigdagTimedMethodInterceptor interceptor = new DigdagTimedMethodInterceptor();
        requestInjection(interceptor);
        binder().bindInterceptor(Matchers.any(), Matchers.annotatedWith(DigdagTimed.class), interceptor);
    }
}

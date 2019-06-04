package io.digdag.server.metrics;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import io.digdag.metrics.DigdagMetrics;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;


public class DigdagMetricsModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        configureMeterRegistry();
        binder().bind(DigdagMetrics.class).toInstance(new DigdagMetrics());
        configureInterceptor();
    }

    /**
     *  Configure MeterRegistry.
     *  As default, JmxMeterRegistry is used.
     *  If want to change MeterRegistry, override this method.
     */
    public void configureMeterRegistry()
    {
        JmxMeterRegistry jmx = new JmxMeterRegistry(new JmxConfig() {
            @Override
            public String get(String key) {
                return null;
            }
            @Override
            public String domain() {
                return "io.digdag";
            }

        }, Clock.SYSTEM);
        binder().bind(MeterRegistry.class).toInstance(
                new CompositeMeterRegistry().add(jmx)
        );
    }

    // Set interceptor for @Timed annotation
    public void configureInterceptor()
    {
        TimedMethodInterceptor interceptor = new TimedMethodInterceptor();
        requestInjection(interceptor);
        binder().bindInterceptor(Matchers.any(), Matchers.annotatedWith(Timed.class), interceptor);

    }

}

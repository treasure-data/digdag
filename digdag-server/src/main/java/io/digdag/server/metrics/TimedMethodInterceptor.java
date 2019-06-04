package io.digdag.server.metrics;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

/**
 * MethodInterceptor for Timed annotation.
 */
public class TimedMethodInterceptor implements MethodInterceptor
{
    @Inject
    MeterRegistry registry;

    @Inject
    public TimedMethodInterceptor()
    {

    }

    public Object invoke(MethodInvocation invocation) throws Throwable {

        Timed timed = invocation.getMethod().getAnnotation(Timed.class);
        String metricsName = timed.value() != null? timed.value() : "timed.unknown";
        Timer.Sample sample = Timer.start(registry);
        Optional<String> exceptionClass = Optional.absent();
        try {
            return invocation.proceed();
        } catch (Exception ex) {
            exceptionClass = Optional.of(ex.getClass().getSimpleName());
            throw ex;
        } finally {
            try {
                Timer.Builder builder = Timer.builder(metricsName)
                        .description(timed.description().isEmpty() ? null : timed.description())
                        .tags(timed.extraTags())
                        .publishPercentileHistogram(timed.histogram())
                        .publishPercentiles(timed.percentiles().length == 0 ? null : timed.percentiles())
                        ;
                if (exceptionClass.isPresent()) {
                    builder = builder.tags("exception", exceptionClass.get());
                }
                sample.stop(builder.register(registry));
            } catch (Exception e) {
                // ignoring on purpose
            }
        }
    }
}

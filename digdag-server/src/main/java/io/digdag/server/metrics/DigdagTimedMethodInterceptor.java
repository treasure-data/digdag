package io.digdag.server.metrics;

import com.google.inject.Inject;
import io.digdag.core.session.Task;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.metrics.DigdagMetrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * MethodInterceptor for Timed annotation.
 */
public class DigdagTimedMethodInterceptor implements MethodInterceptor
{
    private static Logger logger = LoggerFactory.getLogger(DigdagTimedMethodInterceptor.class);
    @Inject
    DigdagMetrics metrics;

    public DigdagTimedMethodInterceptor() { }

    public Object invoke(MethodInvocation invocation)
            throws Throwable
    {
        DigdagTimed timed = invocation.getMethod().getAnnotation(DigdagTimed.class);

        String category = timed.category();
        String value = mkValue(timed, invocation);
        String metricsName = metrics.mkMetricsName(category, value);
        Timer.Sample sample = metrics.timerStart(category);
        try {
            return invocation.proceed();
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                Tags taskTags = timed.taskRequest() ? getTaskRequest(invocation) : Tags.empty();

                metrics.timerStop(category, metricsName, taskTags.and(timed.extraTags()), sample);
            } catch (Exception e) {
                // ignoring on purpose
            }
        }
    }

    protected String mkValue(DigdagTimed timed, MethodInvocation invocation)
    {
        String value = timed.value() != null? timed.value() : "";
        String methodName = invocation.getMethod().getName();
        if (timed.appendMethodName() && methodName != null) {
            value += methodName;
        }

        if (value.compareTo("") == 0) {
            value = "unknown";
        }
        return value;
    }

    /**
     * Get TaskRequest arg. from method and extract as Tags.
     * Only first TaskRequest arg. supported.
     * @param invocation
     * @return
     */
    protected Tags getTaskRequest(MethodInvocation invocation)
    {
        for (Object arg : invocation.getArguments()) {
            if (arg instanceof TaskRequest) {
                TaskRequest request = (TaskRequest) arg;
                return Tags.of("site_id", Integer.toString(request.getSiteId()),
                        "project_id", Integer.toString(request.getProjectId()),
                        "project_name", request.getProjectName().or("unknown"),
                        "workflow_name", request.getWorkflowName()
                );
            }
        }
        return Tags.empty();
    }
}

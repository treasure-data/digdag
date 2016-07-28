package io.digdag.core.workflow;

import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.digdag.core.session.SessionMonitorExecutor;

public class WorkflowExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(WorkflowExecutor.class).in(Scopes.SINGLETON);
        binder.bind(SlaCalculator.class).in(Scopes.SINGLETON);
        binder.bind(AttemptBuilder.class).in(Scopes.SINGLETON);

        // session
        binder.bind(SessionMonitorExecutor.class).asEagerSingleton();
    }
}

package io.digdag.executor;

import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.digdag.core.session.SessionMonitorExecutor;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.SlaCalculator;
import io.digdag.core.workflow.WorkflowExecutor;

public class WorkflowExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(WorkflowExecutor.class).in(Scopes.SINGLETON);
        binder.bind(WorkflowExecutorMain.class).in(Scopes.SINGLETON);
        binder.bind(SlaCalculator.class).in(Scopes.SINGLETON);
        binder.bind(AttemptBuilder.class).in(Scopes.SINGLETON);

        // session
        binder.bind(SessionMonitorExecutor.class).asEagerSingleton();
    }
}

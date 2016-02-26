package io.digdag.core.workflow;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class WorkflowModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(WorkflowExecutor.class).in(Scopes.SINGLETON);
        binder.bind(AttemptBuilder.class).in(Scopes.SINGLETON);
        binder.bind(TaskQueueDispatcher.class).in(Scopes.SINGLETON);
        binder.bind(WorkflowCompiler.class).in(Scopes.SINGLETON);
    }
}

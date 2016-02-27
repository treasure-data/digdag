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
        binder.bind(WorkflowCompiler.class).in(Scopes.SINGLETON);
    }
}

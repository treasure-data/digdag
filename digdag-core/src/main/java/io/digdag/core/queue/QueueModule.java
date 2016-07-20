package io.digdag.core.queue;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.TaskQueueFactory;
import io.digdag.core.database.DatabaseTaskQueueFactory;
import io.digdag.core.workflow.TaskQueueDispatcher;

public class QueueModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TaskQueueServerManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskQueueClientManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskQueueDispatcher.class).to(QueueTaskQueueDispatcher.class).in(Scopes.SINGLETON);

        // built-in queue
        Multibinder<TaskQueueFactory> taskQueueBinder = Multibinder.newSetBinder(binder, TaskQueueFactory.class);
        taskQueueBinder.addBinding().to(DatabaseTaskQueueFactory.class).in(Scopes.SINGLETON);
    }
}

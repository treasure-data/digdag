package io.digdag.core.workflow;

import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.SchedulerFactory;
import io.digdag.spi.OperatorFactory;
import io.digdag.core.DigdagEmbed;

public class WorkflowTestingUtils
{
    private WorkflowTestingUtils() { }

    public static DigdagEmbed setupEmbed()
    {
        return new DigdagEmbed.Bootstrap()
            .withExtensionLoader(false)
            .addModules((binder) -> {
                Multibinder<SchedulerFactory> schedulerBinder = Multibinder.newSetBinder(binder, SchedulerFactory.class);
                // no scheduler

                Multibinder<OperatorFactory> operatorFactoryBinder = Multibinder.newSetBinder(binder, OperatorFactory.class);
                operatorFactoryBinder.addBinding().to(NoopOperatorFactory.class).in(Scopes.SINGLETON);
            })
            .initializeCloseable();
    }
}

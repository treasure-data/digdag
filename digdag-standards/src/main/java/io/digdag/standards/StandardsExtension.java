package io.digdag.standards;

import java.util.List;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.common.collect.ImmutableList;
import io.digdag.core.Extension;
import io.digdag.spi.SchedulerFactory;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.spi.TaskQueueFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.standards.scheduler.CronSchedulerFactory;
import io.digdag.standards.scheduler.MonthlySchedulerFactory;
import io.digdag.standards.scheduler.DailySchedulerFactory;
import io.digdag.standards.scheduler.HourlySchedulerFactory;
import io.digdag.standards.scheduler.MinutesIntervalSchedulerFactory;
import io.digdag.standards.task.PyTaskRunnerFactory;
import io.digdag.standards.task.RbTaskRunnerFactory;
import io.digdag.standards.task.ShTaskRunnerFactory;
import io.digdag.standards.task.MailTaskRunnerFactory;
import io.digdag.standards.task.EmbulkTaskRunnerFactory;
import io.digdag.standards.task.td.TdTaskRunnerFactory;
import io.digdag.standards.task.td.TdDdlTaskRunnerFactory;
import io.digdag.standards.command.SimpleCommandExecutor;
import io.digdag.standards.command.DockerCommandExecutor;

public class StandardsExtension
        implements Extension
{
    public List<Module> getModules()
    {
        return ImmutableList.of(new StandardsModule());
    }

    private static class StandardsModule
        implements Module
    {
        public void configure(Binder binder)
        {
            //binder.bind(CommandExecutor.class).to(SimpleCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(CommandExecutor.class).to(DockerCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(SimpleCommandExecutor.class).in(Scopes.SINGLETON);

            Multibinder<TaskRunnerFactory> taskExecutorBinder = Multibinder.newSetBinder(binder, TaskRunnerFactory.class);
            taskExecutorBinder.addBinding().to(PyTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(RbTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(ShTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(MailTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(EmbulkTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdTaskRunnerFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdDdlTaskRunnerFactory.class).in(Scopes.SINGLETON);

            Multibinder<SchedulerFactory> schedulerBinder = Multibinder.newSetBinder(binder, SchedulerFactory.class);
            schedulerBinder.addBinding().to(CronSchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(MonthlySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(DailySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(HourlySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(MinutesIntervalSchedulerFactory.class).in(Scopes.SINGLETON);
        }
    }
}

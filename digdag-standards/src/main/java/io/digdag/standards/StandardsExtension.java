package io.digdag.standards;

import java.util.List;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.common.collect.ImmutableList;
import io.digdag.core.Extension;
import io.digdag.spi.SchedulerFactory;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.standards.scheduler.CronSchedulerFactory;
import io.digdag.standards.scheduler.MonthlySchedulerFactory;
import io.digdag.standards.scheduler.DailySchedulerFactory;
import io.digdag.standards.scheduler.HourlySchedulerFactory;
import io.digdag.standards.scheduler.MinutesIntervalSchedulerFactory;
import io.digdag.standards.operator.PyOperatorFactory;
import io.digdag.standards.operator.RbOperatorFactory;
import io.digdag.standards.operator.ShOperatorFactory;
import io.digdag.standards.operator.MailOperatorFactory;
import io.digdag.standards.operator.LoopOperatorFactory;
import io.digdag.standards.operator.ForEachOperatorFactory;
import io.digdag.standards.operator.EmbulkOperatorFactory;
import io.digdag.standards.operator.td.TdOperatorFactory;
import io.digdag.standards.operator.td.TdRunOperatorFactory;
import io.digdag.standards.operator.td.TdLoadOperatorFactory;
import io.digdag.standards.operator.td.TdDdlOperatorFactory;
import io.digdag.standards.operator.td.TdTableExportOperatorFactory;
import io.digdag.standards.command.SimpleCommandExecutor;
import io.digdag.standards.command.DockerCommandExecutor;

public class StandardsExtension
        implements Extension
{
    @Override
    public List<Module> getModules()
    {
        return ImmutableList.of(new StandardsModule());
    }

    private static class StandardsModule
        implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            //binder.bind(CommandExecutor.class).to(SimpleCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(CommandExecutor.class).to(DockerCommandExecutor.class).in(Scopes.SINGLETON);
            binder.bind(SimpleCommandExecutor.class).in(Scopes.SINGLETON);

            Multibinder<OperatorFactory> taskExecutorBinder = Multibinder.newSetBinder(binder, OperatorFactory.class);
            taskExecutorBinder.addBinding().to(PyOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(RbOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(ShOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(MailOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(LoopOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(ForEachOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(EmbulkOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdRunOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdLoadOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdDdlOperatorFactory.class).in(Scopes.SINGLETON);
            taskExecutorBinder.addBinding().to(TdTableExportOperatorFactory.class).in(Scopes.SINGLETON);

            Multibinder<SchedulerFactory> schedulerBinder = Multibinder.newSetBinder(binder, SchedulerFactory.class);
            schedulerBinder.addBinding().to(CronSchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(MonthlySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(DailySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(HourlySchedulerFactory.class).in(Scopes.SINGLETON);
            schedulerBinder.addBinding().to(MinutesIntervalSchedulerFactory.class).in(Scopes.SINGLETON);
        }
    }
}

package io.digdag.standards;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.digdag.spi.Extension;
import io.digdag.standards.operator.OperatorModule;
import io.digdag.standards.scheduler.SchedulerModule;
import io.digdag.standards.command.CommandExecutorModule;
import io.digdag.standards.td.TdConfigurationModule;

public class StandardsExtension
        implements Extension
{
    @Override
    public List<Module> getModules()
    {
        return ImmutableList.of(
                new SchedulerModule(),
                new CommandExecutorModule(),
                new OperatorModule(),
                new TdConfigurationModule()
                );
    }
}

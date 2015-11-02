package io.digdag.core;

import java.util.Set;
import java.util.List;
import java.util.Date;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;

public class SchedulerManager
{
    private final List<SchedulerFactory> types;

    @Inject
    public SchedulerManager(Set<SchedulerFactory> factories)
    {
        this.types = ImmutableList.copyOf(factories);
    }

    public Optional<ConfigSource> getSchedulerConfig(WorkflowSource workflow)
    {
        ConfigSource schedulerConfig = workflow.getConfig().getNestedOrGetEmpty("schedule");
        if (schedulerConfig.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(schedulerConfig);
    }

    public Scheduler getScheduler(ConfigSource config)
    {
        for (SchedulerFactory type : types) {
            if (type.matches(config)) {
                return type.newScheduler(config);
            }
        }
        // TODO exception class
        throw new RuntimeException("No scheduler matches with config: "+config);
    }
}

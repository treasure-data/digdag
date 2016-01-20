package io.digdag.core.schedule;

import java.util.Set;
import java.util.List;

import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigException;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.StoredWorkflowSource;
import io.digdag.core.repository.ScheduleSource;

public class SchedulerManager
{
    private final List<SchedulerFactory> types;

    @Inject
    public SchedulerManager(Set<SchedulerFactory> factories)
    {
        this.types = ImmutableList.copyOf(factories);
    }

    public Scheduler getScheduler(Config config)
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

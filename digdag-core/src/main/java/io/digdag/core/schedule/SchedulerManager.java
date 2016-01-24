package io.digdag.core.schedule;

import java.util.Set;
import java.util.List;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinition;

public class SchedulerManager
{
    private final List<SchedulerFactory> types;

    @Inject
    public SchedulerManager(Set<SchedulerFactory> factories)
    {
        this.types = ImmutableList.copyOf(factories);
    }

    // get workflowTimeZone using ScheduleExecutor.getWorkflowTimeZone
    public Scheduler getScheduler(Config config, ZoneId workflowTimeZone)
    {
        for (SchedulerFactory type : types) {
            if (type.matches(config)) {
                return type.newScheduler(config, workflowTimeZone);
            }
        }
        // TODO exception class
        throw new RuntimeException("No scheduler matches with config: "+config);
    }
}

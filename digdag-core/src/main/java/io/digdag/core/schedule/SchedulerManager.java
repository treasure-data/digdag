package io.digdag.core.schedule;

import java.util.Set;
import java.util.Map;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;

public class SchedulerManager
{
    private final Map<String, SchedulerFactory> types;

    @Inject
    public SchedulerManager(Set<SchedulerFactory> factories)
    {
        ImmutableMap.Builder<String, SchedulerFactory> builder = ImmutableMap.builder();
        for (SchedulerFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.types = builder.build();
    }

    public Optional<Scheduler> tryGetScheduler(Revision rev, WorkflowDefinition def)
    {
        return ScheduleExecutor.tryGetScheduleConfig(def).transform(it ->
                    getScheduler(it, ScheduleExecutor.getTimeZoneOfStoredWorkflow(rev, def))
                );
    }

    public Optional<Scheduler> tryGetScheduler(StoredWorkflowDefinitionWithRepository def)
    {
        return ScheduleExecutor.tryGetScheduleConfig(def).transform(it ->
                    getScheduler(it, ScheduleExecutor.getTimeZoneOfStoredWorkflow(def))
                );
    }

    public Scheduler getScheduler(WorkflowDefinition def, ZoneId workflowTimeZone)
    {
        return getScheduler(ScheduleExecutor.getScheduleConfig(def), workflowTimeZone);
    }

    // get workflowTimeZone using ScheduleExecutor.getTimeZoneOfStoredWorkflow
    public Scheduler getScheduler(Config config, ZoneId workflowTimeZone)
    {
        Config c = config.deepCopy();

        String type;
        if (c.has("_type")) {
            type = c.get("_type", String.class);
        }
        else {
            java.util.Optional<String> operatorKey = c.getKeys()
                .stream()
                .filter(key -> key.endsWith(">"))
                .findFirst();
            if (!operatorKey.isPresent()) {
                throw new ConfigException("Schedule config requires 'type>: at' parameter: " + c);
            }
            type = operatorKey.get().substring(0, operatorKey.get().length() - 1);
            Object command = c.get(operatorKey.get(), Object.class);
            c.set("_type", type);
            c.set("_command", command);
        }

        SchedulerFactory factory = types.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown scheduler type: " + type);
        }
        return factory.newScheduler(c, workflowTimeZone);
    }
}

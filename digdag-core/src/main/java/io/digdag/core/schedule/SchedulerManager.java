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
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;

public class SchedulerManager
{
    private static Optional<Config> tryGetScheduleConfig(WorkflowDefinition def)
    {
        return def.getConfig().getOptional("_schedule", Config.class);
    }

    // used only by SchedulerManager and Check command
    public static Config getScheduleConfig(WorkflowDefinition def)
    {
        return def.getConfig().getNested("_schedule");
    }

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
        return tryGetScheduleConfig(def).transform(it ->
                    getScheduler(it, def.getTimeZone())
                );
    }

    public Optional<Scheduler> tryGetScheduler(StoredWorkflowDefinitionWithProject def)
    {
        return tryGetScheduleConfig(def).transform(it ->
                    getScheduler(it, def.getTimeZone())
                );
    }

    // used by ScheduleExecutor which is certain that the workflow has a scheduler
    Scheduler getScheduler(StoredWorkflowDefinition def)
    {
        return getScheduler(getScheduleConfig(def), def.getTimeZone());
    }

    private Scheduler getScheduler(Config schedulerConfig, ZoneId workflowTimeZone)
    {
        Config c = schedulerConfig.deepCopy();

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

package io.digdag.core.schedule;

import java.util.Set;
import java.util.Map;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinition;

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

    // get workflowTimeZone using ScheduleExecutor.getWorkflowTimeZone
    public Scheduler getScheduler(Config config, ZoneId workflowTimeZone)
    {
        Config c = config.deepCopy();

        String type;
        if (c.has("type")) {
            type = c.get("type", String.class);
        }
        else {
            java.util.Optional<String> commandKey = c.getKeys()
                .stream()
                .filter(key -> key.endsWith(">"))
                .findFirst();
            if (!commandKey.isPresent()) {
                throw new ConfigException("Schedule config needs type> key: " + c);
            }
            type = commandKey.get().substring(0, commandKey.get().length() - 1);
            Object command = c.get(commandKey.get(), Object.class);
            c.set("type", type);
            c.set("command", command);
        }

        SchedulerFactory factory = types.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown scheduler type: " + type);
        }
        return factory.newScheduler(c, workflowTimeZone);
    }
}

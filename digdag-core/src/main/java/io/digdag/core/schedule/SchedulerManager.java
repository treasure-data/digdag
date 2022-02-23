package io.digdag.core.schedule;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.time.ZoneId;
import java.util.function.Consumer;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.core.agent.CheckedConfig;
import io.digdag.core.agent.EditDistance;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.core.schedule.ScheduleExecutor.BUILT_IN_SCHEDULE_PARAMS;

class UnusedConfigException extends ConfigException
{
    UnusedConfigException(String message)
    {
        super(message);
    }
}

public class SchedulerManager
{
    private static final Logger logger = LoggerFactory.getLogger(SchedulerManager.class);

    private static Optional<Config> tryGetScheduleConfig(WorkflowDefinition def)
    {
        return def.getConfig().getOptional("schedule", Config.class);
    }

    // used only by SchedulerManager and Check command
    public static Config getScheduleConfig(WorkflowDefinition def)
    {
        return def.getConfig().getNested("schedule");
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
                getScheduler(it, def.getTimeZone(), false)
        );
    }

    public Optional<Scheduler> tryGetScheduler(StoredWorkflowDefinitionWithProject def)
    {
        return tryGetScheduleConfig(def).transform(it ->
                getScheduler(it, def.getTimeZone(), false)
        );
    }

    public Optional<Scheduler> tryGetScheduler(Revision rev, WorkflowDefinition def, boolean throwUnusedKeys)
    {
        return tryGetScheduleConfig(def).transform(it ->
                    getScheduler(it, def.getTimeZone(), throwUnusedKeys)
                );
    }

    // used by ScheduleExecutor which is certain that the workflow has a scheduler
    Scheduler getScheduler(StoredWorkflowDefinition def)
    {
        return getScheduler(getScheduleConfig(def), def.getTimeZone(), false);
    }

    private Scheduler getScheduler(Config schedulerConfig, ZoneId workflowTimeZone, boolean throwUnusedKeys)
    {
        Set<String> shouldBeUsedKeys = new HashSet<>(schedulerConfig.getKeys());
        // Track accessed keys using UsedKeysSet class
        CheckedConfig.UsedKeysSet usedKeys = new CheckedConfig.UsedKeysSet();

        Config c = new CheckedConfig(schedulerConfig, usedKeys);

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

        Optional<Instant> startDate = c.getOptional("start_date", String.class).transform(it -> LocalTimeOrInstant.fromString(it).toInstant(workflowTimeZone));
        Optional<Instant> endDate = c.getOptional("end_date", String.class).transform(it -> LocalTimeOrInstant.fromString(it).toInstant(workflowTimeZone));

        for(String param : BUILT_IN_SCHEDULE_PARAMS){
            usedKeys.add(param);
        }

        SchedulerFactory factory = types.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown scheduler type: " + type);
        }

        if (!usedKeys.isAllUsed()) {
            shouldBeUsedKeys.removeAll(usedKeys);
            if (!shouldBeUsedKeys.isEmpty()) {
                StringBuilder buf = new StringBuilder();
                for (String key: shouldBeUsedKeys) {
                    buf.append(getWarnUnusedKey(key, usedKeys));
                    buf.append("\n");
                }
                if (throwUnusedKeys)
                    throw new UnusedConfigException(buf.toString());
                else
                    logger.error(buf.toString());
            }
        }

        return factory.newScheduler(c, workflowTimeZone, startDate, endDate);
    }

    private String getWarnUnusedKey(String shouldBeUsedButNotUsedKey, Collection<String> candidateKeys)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("Parameter '");
        buf.append(shouldBeUsedButNotUsedKey);
        buf.append("' is not used at schedule. ");

        List<String> suggestions = EditDistance.suggest(shouldBeUsedButNotUsedKey, candidateKeys, 0.50);
        if (!suggestions.isEmpty()) {
            buf.append("> Did you mean '");
            buf.append(suggestions);
            buf.append("'?");
        }

        return buf.toString();
    }
}

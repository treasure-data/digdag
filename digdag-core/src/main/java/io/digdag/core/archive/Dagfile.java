package io.digdag.core.archive;

import java.util.Map;
import java.time.ZoneId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.immutables.value.Value;
import io.digdag.core.repository.ModelValidator;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class Dagfile
{
    // name, timezone, _export, and others
    private static final String[] TOP_LEVEL_CONFIG = new String[] {
        "schedule",
        "sla",
        "_error",
        "_check",
        "_parallel",
    };

    private final String workflowName;

    private final Config tasks;

    private final ZoneId timeZone;

    private final Config topLevelExport;

    private final Config otherTopLevelConfig;

    private Dagfile(
            String workflowName,
            Config tasks,
            ZoneId timeZone,
            Config topLevelExport,
            Config otherTopLevelConfig)
    {
        this.workflowName = workflowName;
        this.tasks = tasks;
        this.timeZone = timeZone;
        this.topLevelExport = topLevelExport;
        this.otherTopLevelConfig = otherTopLevelConfig;
        check();
    }

    protected void check()
    {
        ModelValidator validator = ModelValidator.builder();
        for (String taskName : tasks.getKeys()) {
            validator.checkTaskName("task name", taskName);
        }
        validator
            .checkWorkflowName("name", getWorkflowName())
            .validate("workflow", this);
        // TODO should here validate key names of defaultParams?
    }

    public String getWorkflowName()
    {
        return workflowName;
    }

    public Config getTasks()
    {
        return tasks;
    }

    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    public Config getTopLevelExport()
    {
        return topLevelExport;
    }

    public Config getOtherTopLevelConfig()
    {
        return otherTopLevelConfig;
    }

    public static Dagfile fromConfig(Config config)
    {
        Config copy = config.deepCopy();

        String workflowName = copy.get("name", String.class);
        copy.remove("name");

        ZoneId timeZone = copy.getOptional("timezone", ZoneId.class).or(ZoneId.of("UTC"));
        copy.remove("timezone");

        Config topLevelExport = copy.getNestedOrGetEmpty("_export");
        copy.remove("_export");

        Config otherTopLevelConfig = copy.getFactory().create();
        for (String key : TOP_LEVEL_CONFIG) {
            if (copy.has(key)) {
                otherTopLevelConfig.set(key, copy.getNested(key));
                copy.remove(key);
            }
        }

        Config tasks = config.getFactory().create();
        for (String key : copy.getKeys()) {
            if (key.startsWith("+")) {
                tasks.set(key, copy.get(key, JsonNode.class));
                copy.remove(key);
            }
        }

        if (!copy.isEmpty()) {
            throw new ConfigException("Workflow definition file includes unknown keys: " + copy.getKeys());
        }

        return new Dagfile(
                workflowName,
                tasks,
                timeZone,
                topLevelExport,
                otherTopLevelConfig);
    }

    public Config buildConfig()
    {
        Config config = getTasks().getFactory().create();

        config.set("name", getWorkflowName());

        config.set("timezone", getTimeZone());

        config.set("_export", getTopLevelExport());

        config.setAll(getOtherTopLevelConfig());

        config.setAll(getTasks());

        return config;
    }

    public WorkflowDefinition toWorkflowDefinition()
    {
        return WorkflowDefinition.of(
                getWorkflowName(),
                buildConfig(),
                getTimeZone());
    }
}

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

@Value.Immutable
public abstract class Dagfile
{
    public abstract String getWorkflowName();  // name: workflow-name

    public abstract Config getTasks();  // +name: ...

    public abstract Optional<ZoneId> getTimeZone();  // timezone: UTC

    public abstract Config getDefaultParams();  // _export: ...

    @JsonCreator
    public static Dagfile fromConfig(Config config)
    {
        Config copy = config.deepCopy();

        String workflowName = copy.get("name", String.class);
        copy.remove("name");

        Optional<ZoneId> timeZone = copy.getOptional("timezone", ZoneId.class);
        copy.remove("timezone");

        Config defaultParams = copy.getNestedOrGetEmpty("_export");
        copy.remove("_export");

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

        return ImmutableDagfile.builder()
            .workflowName(workflowName)
            .tasks(tasks)
            .timeZone(timeZone)
            .defaultParams(defaultParams)
            .build();
    }

    @JsonValue
    public Config toConfig()
    {
        Config config = getTasks().getFactory().create();

        config.set("name", getWorkflowName());

        if (getTimeZone().isPresent()) {
            config.set("timezone", getTimeZone().get());
        }

        config.set("_export", getDefaultParams());

        config.setAll(getTasks());

        return config;
    }

    @Value.Check
    protected void check()
    {
        // TODO validate key names of defaultParams?
        ModelValidator.builder()
            .checkTaskName("name", getWorkflowName())
            .validate("workflow", this);
    }
}

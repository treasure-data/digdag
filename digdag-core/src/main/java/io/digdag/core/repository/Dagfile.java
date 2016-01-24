package io.digdag.core.repository;

import java.util.Map;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import java.util.regex.Pattern;

@Value.Immutable
public abstract class Dagfile
{
    public abstract Optional<String> getDefaultTaskName();  // "run"

    public abstract WorkflowDefinitionList getWorkflowList();   // "+..."

    public abstract Config getDefaultParams();  // the other keys

    @JsonCreator
    public static Dagfile fromConfig(Config config)
    {
        Config others = config.deepCopy();
        Config workflowList = config.getFactory().create();

        for (String key : others.getKeys()) {
            if (key.startsWith("+")) {
                workflowList.set(key, others.get(key, JsonNode.class));
                others.remove(key);
            }
        }

        Optional<String> defaultTaskName = config.getOptional("run", String.class);
        others.remove("run");

        return builder()
            .defaultTaskName(defaultTaskName)
            .workflowList(workflowList.convert(WorkflowDefinitionList.class))
            .defaultParams(others)
            .build();
    }

    public static ImmutableDagfile.Builder builder()
    {
        return ImmutableDagfile.builder();
    }

    @JsonValue
    public Map<String, Object> toJsonValue()
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        if (getDefaultTaskName().isPresent()) {
            builder.put("run", getDefaultTaskName().get());
        }

        Config defaultParams = getDefaultParams();
        for (String key : defaultParams.getKeys()) {
            builder.put(key, defaultParams.get(key, JsonNode.class));
        }

        for (WorkflowDefinition source : getWorkflowList().get()) {
            builder.put(source.getName(), source.getConfig());
        }

        return builder.build();
    }

    // check
    // TODO validate default task name begins with +
    // TODO validate key names of defaultParams
}

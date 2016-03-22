package io.digdag.core.repository;

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
import io.digdag.client.config.Config;
import java.util.regex.Pattern;

@Value.Immutable
public abstract class Dagfile
{
    public abstract Optional<String> getDefaultTaskName();  // "run"

    public abstract Optional<ZoneId> getDefaultTimeZone();  // "timezone"

    public abstract WorkflowDefinitionList getWorkflowList();   // "+..."

    public abstract Config getDefaultParams();  // the other keys

    public ArchiveMetadata toArchiveMetadata(ZoneId defaultTimeZone)
    {
        return ArchiveMetadata.of(
                getWorkflowList(),
                getDefaultParams(),
                getDefaultTimeZone().or(defaultTimeZone));
    }

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
        Optional<ZoneId> defaultTimeZone = config.getOptional("timezone", ZoneId.class);
        Config defaultParams = config.getNestedOrGetEmpty("_export");

        return builder()
            .defaultTaskName(defaultTaskName)
            .defaultTimeZone(defaultTimeZone)
            .workflowList(workflowList.convert(WorkflowDefinitionList.class))
            .defaultParams(defaultParams)
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

    @Value.Check
    protected void check()
    {
        // check
        // TODO validate key names of defaultParams
    }
}

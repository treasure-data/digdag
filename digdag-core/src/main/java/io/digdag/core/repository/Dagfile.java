package io.digdag.core.repository;

import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;
import java.util.regex.Pattern;

@Value.Immutable
@JsonDeserialize(as = ImmutableDagfile.class)
public abstract class Dagfile
{
    @JsonProperty("run")
    public abstract Optional<String> getDefaultTaskName();

    @JsonUnwrapped
    public abstract WorkflowSourceList getWorkflowList();

    @JsonUnwrapped
    public abstract ScheduleSourceList getScheduleList();

    @JsonUnwrapped
    public abstract Config getDefaultParams();

    @JsonCreator
    public static Dagfile fromConfig(Config config)
    {
        Config others = config.deepCopy();
        Config workflowList = config.getFactory().create();
        Config scheduleList = config.getFactory().create();

        for (String key : others.getKeys()) {
            if (key.startsWith("+")) {
                workflowList.set(key, others.get(key, JsonNode.class));
                others.remove(key);
            }
            else if (key.startsWith("-")) {
                scheduleList.set(key, others.get(key, JsonNode.class));
                others.remove(key);
            }
            // TODO validate key
        }

        return builder()
            .defaultTaskName(config.getOptional("run", String.class))
            .workflowList(workflowList.convert(WorkflowSourceList.class))
            .scheduleList(scheduleList.convert(ScheduleSourceList.class))
            .defaultParams(others)
            .build();
    }

    public static ImmutableDagfile.Builder builder()
    {
        return ImmutableDagfile.builder();
    }
}

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
    @JsonProperty("default")
    public abstract Optional<String> getDefaultTaskName();

    @JsonUnwrapped
    public abstract WorkflowSourceList getWorkflowList();

    @JsonUnwrapped
    public abstract ScheduleSourceList getScheduleList();

    @JsonCreator
    public static Dagfile fromConfig(Config config)
    {
        Config workflowCopy = config.deepCopy();
        for (String key : workflowCopy.getKeys()) {
            if (!key.startsWith("+")) {
                workflowCopy.remove(key);
            }
        }

        Config scheduleCopy = config.deepCopy();
        for (String key : scheduleCopy.getKeys()) {
            // TODO schedule syntax is not fixed yet
            if (!key.startsWith("-")) {
                scheduleCopy.remove(key);
            }
        }

        return builder()
            .defaultTaskName(config.getOptional("default", String.class))
            .workflowList(workflowCopy.convert(WorkflowSourceList.class))
            .scheduleList(scheduleCopy.convert(ScheduleSourceList.class))
            .build();
    }

    public static ImmutableDagfile.Builder builder()
    {
        return ImmutableDagfile.builder();
    }
}

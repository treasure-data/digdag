package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.time.ZoneId;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.immutables.value.Value;

@Value.Immutable
public abstract class WorkflowDefinitionList
{
    public abstract List<WorkflowDefinition> get();

    public static WorkflowDefinitionList of(List<WorkflowDefinition> list)
    {
        return ImmutableWorkflowDefinitionList.builder().addAllGet(list).build();
    }

    @JsonCreator
    public static WorkflowDefinitionList of(Config object)
    {
        ImmutableList.Builder<WorkflowDefinition> builder = ImmutableList.builder();
        for (String key : object.getKeys()) {
            Config copy = object.getNestedOrderedOrGetEmpty(key).deepCopy();
            ZoneId timeZone = copy.get("timezone", ZoneId.class);
            copy.remove("timezone");
            builder.add(WorkflowDefinition.of(key, copy, timeZone));
        }
        return of(builder.build());
    }

    @JsonValue
    public LinkedHashMap<String, Config> toJson()
    {
        // workflow source list must be an order-preserving map
        LinkedHashMap<String, Config> map = new LinkedHashMap<String, Config>();
        for (WorkflowDefinition wf : get()) {
            Config copy = wf.getConfig().deepCopy();
            copy.set("timezone", wf.getTimeZone());
            map.put(wf.getName(), copy);
        }
        return map;
    }
}

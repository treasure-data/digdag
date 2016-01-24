package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
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
            builder.add(WorkflowDefinition.of(key, object.getNestedOrderedOrGetEmpty(key)));
        }
        return of(builder.build());
    }

    @JsonValue
    public Map<String, Config> toJson()
    {
        // workflow source list must be an order-preserving map
        Map<String, Config> map = new LinkedHashMap<String, Config>();
        for (WorkflowDefinition wf : get()) {
            map.put(wf.getName(), wf.getConfig());
        }
        return map;
    }
}

package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.config.Config;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ScheduleSourceList
{
    // same code with WorkflowSourceList

    public abstract List<ScheduleSource> get();

    public static ScheduleSourceList of(List<ScheduleSource> list)
    {
        return ImmutableScheduleSourceList.builder().addAllGet(list).build();
    }

    @JsonCreator
    public static ScheduleSourceList of(Config object)
    {
        ImmutableList.Builder<ScheduleSource> builder = ImmutableList.builder();
        for (String key : object.getKeys()) {
            builder.add(ScheduleSource.of(key, object.getNestedOrderedOrGetEmpty(key)));
        }
        return of(builder.build());
    }

    @JsonValue
    public Map<String, Config> toJson()
    {
        // schedule source list must be an order-preserving map
        Map<String, Config> map = new LinkedHashMap<String, Config>();
        for (ScheduleSource wf : get()) {
            map.put(wf.getName(), wf.getConfig());
        }
        return map;
    }
}

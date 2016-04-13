package io.digdag.core.repository;

import java.util.Map;
import java.time.ZoneId;
import com.google.common.collect.ImmutableMap;

public class TimeZoneMap
{
    public static TimeZoneMap empty()
    {
        return new TimeZoneMap(ImmutableMap.of());
    }

    private final Map<Long, ZoneId> map;

    public TimeZoneMap(Map<Long, ZoneId> map)
    {
        this.map = map;
    }

    public Map<Long, ZoneId> toMap()
    {
        return ImmutableMap.copyOf(map);
    }

    public ZoneId get(long id)
        throws ResourceNotFoundException
    {
        ZoneId proj = map.get(id);
        if (proj == null) {
            throw new ResourceNotFoundException("timezone of workflow definition id=" + id);
        }
        return proj;
    }
}

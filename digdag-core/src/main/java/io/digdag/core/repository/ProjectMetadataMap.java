package io.digdag.core.repository;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;

import java.util.Collection;
import java.util.List;

public class ProjectMetadataMap
{
    private final LinkedHashMultimap<String, String> map;

    public ProjectMetadataMap(List<String> keyValueList)
    {
        LinkedHashMultimap<String, String> metadata = LinkedHashMultimap.create();
        for (String keyValue : keyValueList) {
            // format should be `key:value`
            String[] kv = keyValue.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid project metadata format: " + keyValue);
            }
            metadata.put(kv[0], kv[1]);
        }
        this.map = metadata;
    }

    public ImmutableMap<String, Collection<String>> toMap()
    {
        return ImmutableMap.copyOf(map.asMap());
    }

    public List<String> getKeys()
    {
        return ImmutableList.copyOf(map.keySet());
    }
}

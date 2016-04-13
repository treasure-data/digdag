package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Optional;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.ResourceNotFoundException;

public class ProjectMap
{
    public static ProjectMap empty()
    {
        return new ProjectMap(ImmutableMap.of());
    }

    private final Map<Integer, StoredProject> map;

    public ProjectMap(Map<Integer, StoredProject> map)
    {
        this.map = map;
    }

    public StoredProject get(int id)
        throws ResourceNotFoundException
    {
        StoredProject proj = map.get(id);
        if (proj == null) {
            throw new ResourceNotFoundException("project id=" + id);
        }
        return proj;
    }
}

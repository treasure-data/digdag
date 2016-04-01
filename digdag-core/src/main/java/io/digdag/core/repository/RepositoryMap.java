package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Optional;
import io.digdag.core.repository.RepositoryStore;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.ResourceNotFoundException;

public class RepositoryMap
{
    public static RepositoryMap empty()
    {
        return new RepositoryMap(ImmutableMap.of());
    }

    private final Map<Integer, StoredRepository> map;

    public RepositoryMap(Map<Integer, StoredRepository> map)
    {
        this.map = map;
    }

    public StoredRepository get(int id)
        throws ResourceNotFoundException
    {
        StoredRepository repo = map.get(id);
        if (repo == null) {
            throw new ResourceNotFoundException("repository id=" + id);
        }
        return repo;
    }
}

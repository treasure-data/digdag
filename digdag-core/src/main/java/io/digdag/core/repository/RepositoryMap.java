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
    public static RepositoryMap get(RepositoryStore rs)
    {
        List<StoredRepository> repos = rs.getRepositories(Integer.MAX_VALUE, Optional.absent());
        ImmutableMap.Builder<Integer, StoredRepository> builder = ImmutableMap.builder();
        for (StoredRepository repo : repos) {
            builder.put(repo.getId(), repo);
        }
        return new RepositoryMap(builder.build());
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

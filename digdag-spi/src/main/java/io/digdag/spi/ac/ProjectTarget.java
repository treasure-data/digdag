package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ProjectTarget
{
    int getSiteId();

    String getName();

    Optional<Integer> getId();

    // TODO better to have revision info?

    static ProjectTarget of(int siteId, String name)
    {
        return of(siteId, name, Optional.absent());
    }

    static ProjectTarget of(int siteId, String name, int id)
    {
        return of(siteId, name, Optional.of(id));
    }

    static ProjectTarget of(int siteId, String name, Optional<Integer> id)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .name(name)
                .id(id)
                .build();
    }
}

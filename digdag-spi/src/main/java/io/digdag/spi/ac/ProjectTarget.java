package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ProjectTarget
        extends Target
{
    int getSiteId();

    Optional<Integer> getId();

    String getName();

    // TODO better to have revision info?

    static ProjectTarget of(int siteId, Optional<Integer> id, String name)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .id(id)
                .name(name)
                .build();
    }
}

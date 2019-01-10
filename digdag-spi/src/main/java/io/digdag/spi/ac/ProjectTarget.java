package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface ProjectTarget
{
    int getSiteId();

    String getName();

    // TODO better to have revision info?

    static ProjectTarget of(int siteId, String name)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .name(name)
                .build();
    }
}

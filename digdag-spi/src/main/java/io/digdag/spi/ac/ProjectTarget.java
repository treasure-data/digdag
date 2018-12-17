package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface ProjectTarget
{
    int getSiteId();

    int getId();

    String getName();

    // TODO better to have revision info?

    public static ProjectTarget of(int siteId, int id, String name)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .id(id)
                .name(name)
                .build();
    }
}

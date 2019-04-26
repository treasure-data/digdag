package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface SiteTarget
{
    int getSiteId();

    static SiteTarget of(int siteId)
    {
        return ImmutableSiteTarget.builder()
                .siteId(siteId)
                .build();
    }
}

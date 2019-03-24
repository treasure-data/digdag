package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ProjectTarget
{
    int getSiteId();

    String getName();

    Optional<String> getSecretKey();

    // TODO better to have revision info?

    static ProjectTarget of(int siteId, String name)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .name(name)
                .secretKey(Optional.absent())
                .build();
    }

    static ProjectTarget of(int siteId, String name, Optional<String> secretKey)
    {
        return ImmutableProjectTarget.builder()
                .siteId(siteId)
                .name(name)
                .secretKey(secretKey)
                .build();
    }
}

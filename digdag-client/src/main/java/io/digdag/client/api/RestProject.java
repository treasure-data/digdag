package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestProject.class)
public interface RestProject
{
    Id getId();

    String getName();

    String getRevision();

    Instant getCreatedAt();

    Instant getUpdatedAt();

    Optional<Instant> getDeletedAt();

    String getArchiveType();

    Optional<byte[]> getArchiveMd5();

    static ImmutableRestProject.Builder builder()
    {
        return ImmutableRestProject.builder();
    }
}

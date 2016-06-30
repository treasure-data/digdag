package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestProject.class)
@JsonDeserialize(as = ImmutableRestProject.class)
public interface RestProject
{
    int getId();

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

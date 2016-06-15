package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestProject.class)
@JsonDeserialize(as = ImmutableRestProject.class)
public abstract class RestProject
{
    public abstract int getId();

    public abstract String getName();

    public abstract String getRevision();

    public abstract Instant getCreatedAt();

    public abstract Instant getUpdatedAt();

    public abstract Optional<Instant> getDeletedAt();

    public abstract String getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();

    public static ImmutableRestProject.Builder builder()
    {
        return ImmutableRestProject.builder();
    }
}

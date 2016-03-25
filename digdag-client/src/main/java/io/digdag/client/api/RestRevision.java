package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestRevision.class)
@JsonDeserialize(as = ImmutableRestRevision.class)
public abstract class RestRevision
{
    public abstract String getRevision();

    public abstract Instant getCreatedAt();

    public abstract String getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();

    public static ImmutableRestRevision.Builder builder()
    {
        return ImmutableRestRevision.builder();
    }
}

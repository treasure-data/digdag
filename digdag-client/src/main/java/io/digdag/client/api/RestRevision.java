package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestRevision.class)
@JsonDeserialize(as = ImmutableRestRevision.class)
public interface RestRevision
{
    String getRevision();

    Instant getCreatedAt();

    String getArchiveType();

    Optional<byte[]> getArchiveMd5();

    static ImmutableRestRevision.Builder builder()
    {
        return ImmutableRestRevision.builder();
    }
}

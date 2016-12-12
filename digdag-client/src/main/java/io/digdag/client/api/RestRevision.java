package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
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

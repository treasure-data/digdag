package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
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

    // userInfo is Optional only to keep backward compatibility. Ideally, it
    // should fill {} (empty object) by default instead of making it Optional.
    Optional<Config> getUserInfo();

    static ImmutableRestRevision.Builder builder()
    {
        return ImmutableRestRevision.builder();
    }
}

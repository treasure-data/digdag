package io.digdag.spi;

import java.time.Instant;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableStorageObjectSummary.class)
@JsonDeserialize(as = ImmutableStorageObjectSummary.class)
public interface StorageObjectSummary
{
    String getKey();

    long getContentLength();

    Instant getLastModified();

    public static ImmutableStorageObjectSummary.Builder builder()
    {
        return ImmutableStorageObjectSummary.builder();
    }
}

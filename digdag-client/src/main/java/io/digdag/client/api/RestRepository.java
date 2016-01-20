package io.digdag.client.api;

import java.util.Date;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestRepository.class)
@JsonDeserialize(as = ImmutableRestRepository.class)
public abstract class RestRepository
{
    public abstract int getId();

    public abstract String getName();

    public abstract String getRevision();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();

    public abstract String getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();  // TODO Optional is not working correctly but using for now

    public static ImmutableRestRepository.Builder builder()
    {
        return ImmutableRestRepository.builder();
    }
}

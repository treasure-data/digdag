package io.digdag.server;

import java.util.Date;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;

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

    public abstract Optional<byte[]> getArchiveMd5();

    public static ImmutableRestRepository.Builder builder()
    {
        return ImmutableRestRepository.builder();
    }

    public static RestRepository of(StoredRepository repo, StoredRevision rev)
    {
        return builder()
            .id(repo.getId())
            .name(repo.getName())
            .revision(rev.getName())
            .createdAt(repo.getCreatedAt())
            .updatedAt(rev.getCreatedAt())
            .archiveType(rev.getArchiveType())
            .archiveMd5(rev.getArchiveMd5())
            .build();
    }
}

package io.digdag.core.repository;

import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredProjectWithRevision.class)
@JsonDeserialize(as = ImmutableStoredProjectWithRevision.class)
public abstract class StoredProjectWithRevision
        extends Project
{
    public abstract int getId();

    public abstract int getSiteId();

    public abstract Instant getCreatedAt();

    public abstract Optional<Instant> getDeletedAt();

    public abstract String getRevisionName();

    public abstract Instant getRevisionCreatedAt();

    public abstract ArchiveType getRevisionArchiveType();

    public abstract Optional<byte[]> getRevisionArchiveMd5();

    public static StoredProjectWithRevision of(StoredProject proj, StoredRevision rev)
    {
        return ImmutableStoredProjectWithRevision.builder()
            .from((Project)proj)
            .id(proj.getId())
            .siteId(proj.getSiteId())
            .createdAt(proj.getCreatedAt())
            .deletedAt(proj.getDeletedAt())
            .revisionName(rev.getName())
            .revisionCreatedAt(rev.getCreatedAt())
            .revisionArchiveType(rev.getArchiveType())
            .revisionArchiveMd5(rev.getArchiveMd5())
            .build();
    }
}

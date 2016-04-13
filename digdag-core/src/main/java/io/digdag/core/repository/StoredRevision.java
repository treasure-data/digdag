package io.digdag.core.repository;

import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredProject.class)
@JsonDeserialize(as = ImmutableStoredProject.class)
public abstract class StoredRevision
        extends Revision
{
    public abstract int getId();

    public abstract int getProjectId();

    public abstract Instant getCreatedAt();
}

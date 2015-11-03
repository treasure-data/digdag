package io.digdag.core.repository;

import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredRepository.class)
@JsonDeserialize(as = ImmutableStoredRepository.class)
public abstract class StoredRepository
        extends Repository
{
    public abstract int getId();

    public abstract int getSiteId();

    //public abstract Optional<Integer> getLatestRevisionId();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();
}

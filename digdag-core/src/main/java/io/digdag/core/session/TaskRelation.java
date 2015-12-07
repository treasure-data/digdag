package io.digdag.core.session;

import java.util.List;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskRelation.class)
@JsonDeserialize(as = ImmutableTaskRelation.class)
public abstract class TaskRelation
{
    public abstract long getId();

    public abstract Optional<Long> getParentId();

    public abstract List<Long> getUpstreams();
}

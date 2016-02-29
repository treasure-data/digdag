package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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

    public static TaskRelation ofRoot(long id)
    {
        return of(id, Optional.absent(), ImmutableList.of());
    }

    public static TaskRelation of(long id, long parentId, List<Long> upstreams)
    {
        return of(id, Optional.of(parentId), upstreams);
    }

    public static TaskRelation of(long id, Optional<Long> parentId, List<Long> upstreams)
    {
        return ImmutableTaskRelation.builder()
            .id(id)
            .parentId(parentId)
            .upstreams(upstreams)
            .build();
    }
}

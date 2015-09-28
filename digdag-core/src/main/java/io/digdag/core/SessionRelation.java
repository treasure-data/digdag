package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableSessionRelation.class)
@JsonDeserialize(as = ImmutableSessionRelation.class)
public abstract class SessionRelation
{
    public abstract Optional<Integer> getRepositoryId();

    public abstract Optional<Integer> getWorkflowId();

    public static ImmutableSessionRelation.Builder sessionRelationBuilder()
    {
        return ImmutableSessionRelation.builder();
    }

    public static SessionRelation of(Optional<Integer> repositoryId, Optional<Integer> workflowId)
    {
        return sessionRelationBuilder()
            .repositoryId(repositoryId)
            .workflowId(workflowId)
            .build();
    }

    @Value.Check
    protected void check()
    {
        checkState(!getWorkflowId().isPresent() || getRepositoryId().isPresent(),  "repositoryId must be set if workflowId is set");
    }
}

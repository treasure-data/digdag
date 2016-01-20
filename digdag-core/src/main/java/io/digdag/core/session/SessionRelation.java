package io.digdag.core.session;

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
    public abstract int getRepositoryId();

    public abstract int getRevisionId();

    public abstract Optional<Integer> getWorkflowSourceId();

    public static ImmutableSessionRelation.Builder builder()
    {
        return ImmutableSessionRelation.builder();
    }

    public static SessionRelation ofRevision(int repositoryId, int revisionId)
    {
        return builder()
            .repositoryId(repositoryId)
            .revisionId(revisionId)
            .workflowSourceId(Optional.absent())
            .build();
    }

    public static SessionRelation ofWorkflow(int repositoryId, int revisionId, int workflowSourceId)
    {
        return builder()
            .repositoryId(repositoryId)
            .revisionId(revisionId)
            .workflowSourceId(Optional.of(workflowSourceId))
            .build();
    }
}

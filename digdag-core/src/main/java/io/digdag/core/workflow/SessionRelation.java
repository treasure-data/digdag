package io.digdag.core.workflow;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;

@Value.Immutable
@JsonSerialize(as = ImmutableSessionRelation.class)
@JsonDeserialize(as = ImmutableSessionRelation.class)
public abstract class SessionRelation
{
    public abstract int getRepositoryId();

    public abstract int getRevisionId();

    public abstract Optional<Integer> getWorkflowId();

    public static ImmutableSessionRelation.Builder builder()
    {
        return ImmutableSessionRelation.builder();
    }

    public static SessionRelation ofRevision(int repositoryId, int revisionId)
    {
        return builder()
            .repositoryId(repositoryId)
            .revisionId(revisionId)
            .workflowId(Optional.absent())
            .build();
    }

    public static SessionRelation ofWorkflow(int repositoryId, int revisionId, int workflowId)
    {
        return builder()
            .repositoryId(repositoryId)
            .revisionId(revisionId)
            .workflowId(Optional.of(workflowId))
            .build();
    }
}

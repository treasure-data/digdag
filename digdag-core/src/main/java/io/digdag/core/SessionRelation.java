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
    public abstract int getSiteId();

    public abstract Optional<Integer> getRepositoryId();

    public abstract Optional<Integer> getWorkflowId();

    public static ImmutableSessionRelation.Builder builder()
    {
        return ImmutableSessionRelation.builder();
    }

    public static SessionRelation ofWorkflow(int siteId, int repositoryId, int workflowId)
    {
        return builder()
            .siteId(siteId)
            .repositoryId(Optional.of(repositoryId))
            .workflowId(Optional.of(workflowId))
            .build();
    }

    public static SessionRelation ofRepository(int siteId, int repositoryId)
    {
        return builder()
            .siteId(siteId)
            .repositoryId(Optional.of(repositoryId))
            .workflowId(Optional.absent())
            .build();
    }

    public static SessionRelation ofSite(int siteId)
    {
        return builder()
            .siteId(siteId)
            .repositoryId(Optional.absent())
            .workflowId(Optional.absent())
            .build();
    }

    @Value.Check
    protected void check()
    {
        checkState(!getWorkflowId().isPresent() || getRepositoryId().isPresent(),  "repositoryId must be set if workflowId is set");
    }
}

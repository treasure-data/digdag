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
@JsonSerialize(as = ImmutableSessionNamespace.class)
@JsonDeserialize(as = ImmutableSessionNamespace.class)
public abstract class SessionNamespace
{
    public abstract Optional<Integer> getRepositoryId();

    public abstract Optional<Integer> getWorkflowId();

    public static ImmutableSessionNamespace.Builder sessionNamespaceBuilder()
    {
        return ImmutableSessionNamespace.builder();
    }

    public static SessionNamespace ofWorkflow(int repositoryId, int workflowId)
    {
        return sessionNamespaceBuilder()
            .repositoryId(Optional.of(repositoryId))
            .workflowId(Optional.of(workflowId))
            .build();
    }

    public static SessionNamespace ofRepository(int repositoryId)
    {
        return sessionNamespaceBuilder()
            .repositoryId(Optional.of(repositoryId))
            .workflowId(Optional.absent())
            .build();
    }

    public static SessionNamespace ofSite()
    {
        return sessionNamespaceBuilder()
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

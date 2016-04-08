package io.digdag.core.session;

import java.time.Instant;
import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.PackageName;
import io.digdag.core.repository.WorkflowDefinition;

@JsonDeserialize(as = ImmutableSession.class)
public abstract class Session
{
    // TODO to support one-time non-stored workflows, this should be Optional<Integer>. See also AttemptRequest.getStored.
    public abstract int getRepositoryId();

    public abstract PackageName getPackageName();

    public abstract String getWorkflowName();

    public abstract Instant getSessionTime();

    public static Session of(int repositoryId, PackageName packageName, String workflowName, Instant sessionTime)
    {
        return ImmutableSession.builder()
            .repositoryId(repositoryId)
            .packageName(packageName)
            .workflowName(workflowName)
            .sessionTime(sessionTime)
            .build();
    }
}

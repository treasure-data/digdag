package io.digdag.core.session;

import java.time.Instant;
import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinition;

@JsonDeserialize(as = ImmutableSession.class)
public abstract class Session
{
    public abstract int getRepositoryId();

    public abstract String getWorkflowName();

    public abstract Instant getSessionTime();

    public static Session of(int repositoryId, String workflowName, Instant sessionTime)
    {
        return ImmutableSession.builder()
            .repositoryId(repositoryId)
            .workflowName(workflowName)
            .sessionTime(sessionTime)
            .build();
    }
}

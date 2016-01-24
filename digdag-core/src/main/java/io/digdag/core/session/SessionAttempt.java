package io.digdag.core.session;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableSessionAttempt.class)
public abstract class SessionAttempt
{
    public abstract String getAttemptName();

    public abstract Optional<Long> getWorkflowDefinitionId();

    public abstract Config getParams();

    public static SessionAttempt ofStoredWorkflowDefinition(String attemptName, Config params, long storedWorkflowDefinitionId)
    {
        return ImmutableSessionAttempt.builder()
            .attemptName(attemptName)
            .params(params)
            .workflowDefinitionId(Optional.of(storedWorkflowDefinitionId))
            .build();
    }

    public static SessionAttempt ofOneShotWorkflow(String attemptName, Config params)
    {
        return ImmutableSessionAttempt.builder()
            .attemptName(attemptName)
            .params(params)
            .workflowDefinitionId(Optional.absent())
            .build();
    }

    public static SessionAttempt of(String attemptName, Config params, Optional<Long> storedWorkflowDefinitionId)
    {
        return ImmutableSessionAttempt.builder()
            .attemptName(attemptName)
            .params(params)
            .workflowDefinitionId(storedWorkflowDefinitionId)
            .build();
    }
}

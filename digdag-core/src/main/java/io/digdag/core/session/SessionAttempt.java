package io.digdag.core.session;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableSessionAttempt.class)
public abstract class SessionAttempt
{
    public abstract Optional<String> getRetryAttemptName();

    public abstract Optional<Long> getWorkflowDefinitionId();

    public abstract Config getParams();

    public static SessionAttempt ofStoredWorkflowDefinition(Optional<String> retryAttemptName, Config params, long storedWorkflowDefinitionId)
    {
        return ImmutableSessionAttempt.builder()
            .retryAttemptName(retryAttemptName)
            .params(params)
            .workflowDefinitionId(Optional.of(storedWorkflowDefinitionId))
            .build();
    }

    public static SessionAttempt ofOneShotWorkflow(Optional<String> retryAttemptName, Config params)
    {
        return ImmutableSessionAttempt.builder()
            .retryAttemptName(retryAttemptName)
            .params(params)
            .workflowDefinitionId(Optional.absent())
            .build();
    }

    public static SessionAttempt of(Optional<String> retryAttemptName, Config params, Optional<Long> storedWorkflowDefinitionId)
    {
        return ImmutableSessionAttempt.builder()
            .retryAttemptName(retryAttemptName)
            .params(params)
            .workflowDefinitionId(storedWorkflowDefinitionId)
            .build();
    }
}

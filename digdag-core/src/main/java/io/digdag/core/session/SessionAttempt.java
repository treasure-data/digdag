package io.digdag.core.session;

import java.time.ZoneId;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ModelValidator;

@JsonDeserialize(as = ImmutableSessionAttempt.class)
public abstract class SessionAttempt
{
    public abstract Optional<String> getRetryAttemptName();

    public abstract Optional<Long> getWorkflowDefinitionId();

    public abstract ZoneId getTimeZone();

    public abstract Config getParams();

    public static SessionAttempt ofStoredWorkflowDefinition(Optional<String> retryAttemptName, Config params, ZoneId timeZone, long storedWorkflowDefinitionId)
    {
        return of(retryAttemptName, params, timeZone,
                Optional.of(storedWorkflowDefinitionId));
    }

    public static SessionAttempt ofOneShotWorkflow(Optional<String> retryAttemptName, ZoneId timeZone, Config params)
    {
        return of(retryAttemptName, params, timeZone, Optional.absent());
    }

    public static SessionAttempt of(Optional<String> retryAttemptName, Config params, ZoneId timeZone, Optional<Long> storedWorkflowDefinitionId)
    {
        return ImmutableSessionAttempt.builder()
            .retryAttemptName(retryAttemptName)
            .workflowDefinitionId(storedWorkflowDefinitionId)
            .timeZone(timeZone)
            .params(params)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator validator = ModelValidator.builder();
        if (getRetryAttemptName().isPresent()) {
            validator.checkIdentifierName("retry attempt name", getRetryAttemptName().get());
        }
        validator.validate("session attempt", this);
    }
}

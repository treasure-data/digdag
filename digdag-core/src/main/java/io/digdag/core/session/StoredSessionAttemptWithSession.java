package io.digdag.core.session;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSessionAttemptWithSession.class)
@JsonDeserialize(as = ImmutableStoredSessionAttemptWithSession.class)
public abstract class StoredSessionAttemptWithSession
        extends StoredSessionAttempt
{
    public abstract int getSiteId();

    public abstract Session getSession();

    //public abstract Optional<String> getRevisionName();

    public static StoredSessionAttemptWithSession of(int siteId, Session session, StoredSessionAttempt attempt)
    {
        return ImmutableStoredSessionAttemptWithSession.builder()
            .id(attempt.getId())
            .retryAttemptName(attempt.getRetryAttemptName())
            .workflowDefinitionId(attempt.getWorkflowDefinitionId())
            .params(attempt.getParams())
            .stateFlags(attempt.getStateFlags())
            .sessionId(attempt.getSessionId())
            .createdAt(attempt.getCreatedAt())
            .siteId(siteId)
            .session(session)
            .build();
    }
}

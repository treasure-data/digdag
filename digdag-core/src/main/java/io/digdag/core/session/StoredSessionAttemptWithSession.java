package io.digdag.core.session;

import java.util.UUID;
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

    public abstract UUID getSessionUuid();

    public abstract Session getSession();

    //public abstract Optional<String> getRevisionName();

    public static StoredSessionAttemptWithSession of(int siteId, StoredSession session, StoredSessionAttempt attempt)
    {
        return of(siteId, session.getUuid(), ImmutableSession.builder().from(session).build(), attempt);
    }

    public static StoredSessionAttemptWithSession of(int siteId, UUID sessionUuid, Session session, StoredSessionAttempt attempt)
    {
        return ImmutableStoredSessionAttemptWithSession.builder()
            .id(attempt.getId())
            .retryAttemptName(attempt.getRetryAttemptName())
            .workflowDefinitionId(attempt.getWorkflowDefinitionId())
            .timeZone(attempt.getTimeZone())
            .params(attempt.getParams())
            .stateFlags(attempt.getStateFlags())
            .sessionId(attempt.getSessionId())
            .index(attempt.getIndex())
            .createdAt(attempt.getCreatedAt())
            .siteId(siteId)
            .sessionUuid(sessionUuid)
            .session(session)
            .build();
    }

    // used by ScheduleExecutor to create a dry-run dummy session attempt
    public static StoredSessionAttemptWithSession dryRunDummy(int siteId, Session session, StoredSessionAttempt attempt)
    {
        // TODO use 00000000-0000-0000-0000-000000000000 instead of randomUUID if possible
        return of(siteId, UUID.randomUUID(), session, attempt);
    }
}

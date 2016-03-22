package io.digdag.core.workflow;

import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.session.StoredSessionAttemptWithSession;

public class SessionAttemptConflictException
    extends ResourceConflictException
{
    private StoredSessionAttemptWithSession conflictedSession;

    public SessionAttemptConflictException(String message, ResourceConflictException cause, StoredSessionAttemptWithSession conflictedSession)
    {
        super(message, cause);
        this.conflictedSession = conflictedSession;
    }

    public StoredSessionAttemptWithSession getConflictedSession()
    {
        return conflictedSession;
    }
}

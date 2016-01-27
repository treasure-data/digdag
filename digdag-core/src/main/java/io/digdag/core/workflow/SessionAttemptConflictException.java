package io.digdag.core.workflow;

import io.digdag.core.session.StoredSessionAttemptWithSession;

public class SessionAttemptConflictException
    extends Exception
{
    private StoredSessionAttemptWithSession conflictedSession;

    public SessionAttemptConflictException(String message, StoredSessionAttemptWithSession conflictedSession)
    {
        super(message);
        this.conflictedSession = conflictedSession;
    }

    public StoredSessionAttemptWithSession getConflictedSession()
    {
        return conflictedSession;
    }
}

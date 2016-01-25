package io.digdag.core.workflow;

import java.util.List;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.session.SessionAttemptControlStore;
import io.digdag.core.repository.ResourceConflictException;

public class SessionAttemptControl
{
    private final SessionAttemptControlStore store;
    private final long attemptId;

    public SessionAttemptControl(SessionAttemptControlStore store, long attemptId)
    {
        this.store = store;
        this.attemptId = attemptId;
    }

    public void archiveTasks(ObjectMapper mapper, boolean success)
    {
        int n = store.aggregateAndInsertTaskArchive(attemptId);
        int deleted = store.deleteAllTasksOfAttempt(attemptId);
        if (n != deleted) {
            throw new IllegalStateException("Invalid database state");
        }
        store.setDoneToAttemptState(attemptId, success);
    }
}

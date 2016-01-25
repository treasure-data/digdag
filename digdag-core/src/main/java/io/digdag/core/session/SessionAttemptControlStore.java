package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;

public interface SessionAttemptControlStore
{
    int aggregateAndInsertTaskArchive(long attemptId);

    int deleteAllTasksOfAttempt(long attemptId);

    boolean setDoneToAttemptState(long attemptId, boolean success);
}

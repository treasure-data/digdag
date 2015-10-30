package io.digdag.core;

import java.util.List;
import com.google.common.base.*;

public interface SessionStore
        extends Store
{
    List<StoredSession> getAllSessions();  // TODO only for testing

    List<StoredSession> getSessions(int pageSize, Optional<Long> lastId);

    StoredSession getSessionById(long sesId);

    TaskStateCode getRootState(long sesId);

    List<StoredTask> getAllTasks();  // TODO only for testing

    List<StoredTask> getTasks(long sesId, int pageSize, Optional<Long> lastId);
}

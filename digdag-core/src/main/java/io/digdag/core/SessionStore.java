package io.digdag.core;

import java.util.List;
import com.google.common.base.*;

public interface SessionStore
        extends Store
{
    List<StoredSession> getAllSessions();  // TODO only for testing

    List<StoredSession> getSessions(int pageSize, Optional<Long> lastId);

    //List<StoredSession> getSessionsOfRepository(int pageSize, int repoId, Optional<Long> lastId);

    //List<StoredSession> getSessionsOfWorkflow(int pageSize, int workflowId, Optional<Long> lastId);

    StoredSession getSessionById(long sesId);

    //TaskControl getTaskControl(long sesId);

    //Pageable<StoredTask> getTasks(long sesId, int pageSize);

    //StoredTask getTaskById(long taskId);

    //long addTask(long sesId, Task task);
}

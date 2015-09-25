package io.digdag.core;

import java.util.List;
import com.google.common.base.*;

public interface SessionStore
        extends Store
{
    List<StoredSession> getAllSessions();  // TODO only for testing

    List<StoredSession> getSessions(int pageSize, Optional<Long> lastId);  // TODO criteria like workflow_id?

    StoredSession getSessionById(long sesId);

    StoredSession getSessionByName(String name);

    StoredSession putSession(Session session);

    //TaskControl getTaskControl(long sesId);

    //Pageable<StoredTask> getTasks(long sesId, int pageSize);

    //StoredTask getTaskById(long taskId);

    //long addTask(long sesId, Task task);
}

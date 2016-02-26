package io.digdag.core.log;

import com.google.inject.Inject;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;

public class TaskLoggerManager
{
    // TODO this returns a TaskLogger that calls PUT /api/logs/{attempt_id}/files.
    //public TaskLogger getRestTaskLogger(int siteId, long attemptId, String taskName)
}

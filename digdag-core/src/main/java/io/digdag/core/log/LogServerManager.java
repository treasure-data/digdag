package io.digdag.core.log;

import com.google.inject.Inject;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;

public class LogServerManager
{
    private final LogServerFactory factory;

    @Inject
    public LogServerManager(LogServerFactory factory)
    {
        this.factory = factory;
    }

    public LogServer getLogServer()
    {
        return factory.getLogServer();
    }

    public TaskLogger getInProcessTaskLogger(int siteId)
    {
        // TODO implement buffered logger that ends logs to TaskLogger taken from LogServerManager.getInProcessTaskLogger()
        // TODO implement LogServerFactory based on database or local file
        return new NullTaskLogger();
    }

    public static LogFilePrefix logFilePrefixFromSessionAttempt(
            StoredSessionAttemptWithSession attempt)
    {
        return LogFilePrefix.builder()
            .siteId(attempt.getSiteId())
            //.repositoryName()  // TODO
            .workflowName(attempt.getSession().getWorkflowName())
            .sessionTime(attempt.getSession().getSessionTime())
            .timeZone(attempt.getTimeZone())
            .retryAttemptName(attempt.getRetryAttemptName())
            .createdAt(attempt.getCreatedAt())
            .build();
    }
}

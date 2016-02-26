package io.digdag.core.log;

import java.util.Set;
import com.google.inject.Inject;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.core.log.NullLogServerFactory.NullLogServer;
import io.digdag.core.log.LocalFileLogServerFactory.LocalFileLogServer;

public class LogServerManager
{
    private final LogServer logServer;

    @Inject
    public LogServerManager(Set<LogServerFactory> factories,
            ConfigElement ce, ConfigFactory cf)
    {
        Config systemConfig = ce.toConfig(cf);
        String logServerType = systemConfig.get("log-server.type", String.class, "null");
        LogServerFactory factory = findLogServer(factories, logServerType);
        this.logServer = factory.getLogServer(systemConfig);
    }

    private static LogServerFactory findLogServer(Set<LogServerFactory> factories, String type)
    {
        for (LogServerFactory factory : factories) {
            if (type.equals(factory.getType())) {
                return factory;
            }
        }
        throw new ConfigException("Unknown log server type: "+type);
    }

    public LogServer getLogServer()
    {
        return logServer;
    }

    public TaskLogger newInProcessTaskLogger(LogFilePrefix prefix, String taskName)
    {
        if (logServer instanceof NullLogServer) {
            return new NullTaskLogger();
        }
        else if (logServer instanceof LocalFileLogServer) {
            return ((LocalFileLogServer) logServer).newDirectTaskLogger(prefix, taskName);
        }
        else {
            // TODO implement buffered logger that sends logs to getLogServer()
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    public static LogFilePrefix logFilePrefixFromSessionAttempt(
            StoredSessionAttemptWithSession attempt)
    {
        return LogFilePrefix.builder()
            .siteId(attempt.getSiteId())
            .repositoryName("default")  // TODO not available from StoredSessionAttemptWithSession!
            .workflowName(attempt.getSession().getWorkflowName())
            .sessionTime(attempt.getSession().getSessionTime())
            .timeZone(attempt.getTimeZone())
            .retryAttemptName(attempt.getRetryAttemptName())
            .createdAt(attempt.getCreatedAt())
            .build();
    }
}

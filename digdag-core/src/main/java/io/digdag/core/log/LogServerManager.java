package io.digdag.core.log;

import java.util.Set;
import com.google.inject.Inject;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigException;
import io.digdag.core.agent.AgentId;
import io.digdag.core.log.NullLogServerFactory.NullLogServer;
import io.digdag.core.log.LocalFileLogServerFactory.LocalFileLogServer;
import io.digdag.core.TempFileManager;

public class LogServerManager
{
    private final LogServer logServer;
    private final TempFileManager tempFiles;

    @Inject
    public LogServerManager(Set<LogServerFactory> factories, Config systemConfig, TempFileManager tempFiles)
    {
        String logServerType = systemConfig.get("log-server.type", String.class, "null");
        LogServerFactory factory = findLogServer(factories, logServerType);
        this.logServer = factory.getLogServer();
        this.tempFiles = tempFiles;
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

    // this is called when server == agent (server runs a local agent).
    public TaskLogger newInProcessTaskLogger(AgentId agentId, LogFilePrefix prefix, String taskName)
    {
        if (logServer instanceof NullLogServer) {
            return new NullTaskLogger();
        }
        else if (logServer instanceof LocalFileLogServer) {
            return ((LocalFileLogServer) logServer).newDirectTaskLogger(prefix, taskName);
        }
        else {
            return new BufferedRemoteTaskLogger(tempFiles, taskName,
                    (firstLogTime, gzData) -> {
                        logServer.putFile(prefix, taskName, firstLogTime, agentId.toString(), gzData);
                    });
        }
    }

    public static LogFilePrefix logFilePrefixFromSessionAttempt(
            StoredSessionAttemptWithSession attempt)
    {
        return LogFilePrefix.builder()
            .siteId(attempt.getSiteId())
            .repositoryId(attempt.getSession().getRepositoryId())
            .workflowName(attempt.getSession().getWorkflowName())
            .sessionTime(attempt.getSession().getSessionTime())
            .timeZone(attempt.getTimeZone())
            .retryAttemptName(attempt.getRetryAttemptName())
            .createdAt(attempt.getCreatedAt())
            .build();
    }
}

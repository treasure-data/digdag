package io.digdag.core.agent;

import io.digdag.client.config.Config;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableAgentConfig.class)
@JsonDeserialize(as = ImmutableAgentConfig.class)
public interface AgentConfig
{
    int DEFAULT_HEARTBEAT_INTERVAL = 60;
    int DEFAULT_STUCK_TASK_DETECT_TIME = 300;
    int DEFAULT_LOCK_RETENTION_TIME = 300;
    int DEFAULT_MAX_TASK_THREADS = 0;

    boolean getEnabled();

    int getHeartbeatInterval();

    int getStuckTaskDetectTime();

    int getLockRetentionTime();

    int getMaxThreads();

    static ImmutableAgentConfig.Builder defaultBuilder()
    {
        return ImmutableAgentConfig.builder()
            .enabled(true)
            .heartbeatInterval(DEFAULT_HEARTBEAT_INTERVAL)
            .stuckTaskDetectTime(DEFAULT_STUCK_TASK_DETECT_TIME)
            .lockRetentionTime(DEFAULT_LOCK_RETENTION_TIME)
            .maxThreads(DEFAULT_MAX_TASK_THREADS);
    }

    static AgentConfig convertFrom(Config config)
    {
        return defaultBuilder()
            .enabled(config.get("agent.enabled", boolean.class, true))
            .heartbeatInterval(config.get("agent.heartbeat-interval", int.class, DEFAULT_HEARTBEAT_INTERVAL))
            .stuckTaskDetectTime(config.get("agent.stuck-task-detect-time", int.class, DEFAULT_STUCK_TASK_DETECT_TIME))
            .lockRetentionTime(config.get("agent.lock-retention-time", int.class, DEFAULT_LOCK_RETENTION_TIME))
            .maxThreads(config.get("agent.max-task-threads", int.class, DEFAULT_MAX_TASK_THREADS))
            .build();
    }
}

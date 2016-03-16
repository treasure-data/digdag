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
    static final int DEFAULT_HEARTBEAT_INTERVAL = 60;
    static final int DEFAULT_LOCK_RETENTION_TIME = 300;

    int getHeartbeatInterval();

    int getLockRetentionTime();

    static ImmutableAgentConfig.Builder defaultBuilder()
    {
        return ImmutableAgentConfig.builder()
            .heartbeatInterval(DEFAULT_HEARTBEAT_INTERVAL)
            .lockRetentionTime(DEFAULT_LOCK_RETENTION_TIME);
    }

    static AgentConfig convertFrom(Config config)
    {
        return defaultBuilder()
            .heartbeatInterval(config.get("agent.heartbeatInterval", int.class, DEFAULT_HEARTBEAT_INTERVAL))
            .lockRetentionTime(config.get("agent.lockRetentionTime", int.class, DEFAULT_LOCK_RETENTION_TIME))
            .build();
    }
}

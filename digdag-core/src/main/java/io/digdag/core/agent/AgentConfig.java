package io.digdag.core.agent;

import io.digdag.client.config.Config;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableAgentConfig.class)
@JsonDeserialize(as = ImmutableAgentConfig.class)
public abstract class AgentConfig
{
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 60;
    private static final int DEFAULT_LOCK_RETENTION_TIME = 300;

    public abstract int getHeartbeatInterval();

    public abstract int getLockRetentionTime();

    private static ImmutableAgentConfig.Builder defaultBuilder()
    {
        return ImmutableAgentConfig.builder()
            .heartbeatInterval(DEFAULT_HEARTBEAT_INTERVAL)
            .lockRetentionTime(DEFAULT_LOCK_RETENTION_TIME);
    }

    public static AgentConfig convertFrom(Config config)
    {
        return defaultBuilder()
            .heartbeatInterval(config.get("agent.heartbeatInterval", int.class, DEFAULT_HEARTBEAT_INTERVAL))
            .lockRetentionTime(config.get("agent.lockRetentionTime", int.class, DEFAULT_LOCK_RETENTION_TIME))
            .build();
    }
}

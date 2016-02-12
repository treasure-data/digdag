package io.digdag.core.agent;

import java.lang.management.ManagementFactory;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;

public class AgentIdProvider
    implements Provider<AgentId>
{
    private final AgentId id;

    @Inject
    public AgentIdProvider(ConfigElement ce, ConfigFactory cf)
    {
        Config systemConfig = ce.toConfig(cf);
        String id = systemConfig.get("agent.id", String.class, null);
        if (id == null) {
            // get <pid>@<hostname>
            id = ManagementFactory.getRuntimeMXBean().getName();
        }
        this.id = AgentId.of(id);
    }

    @Override
    public AgentId get()
    {
        return id;
    }
}

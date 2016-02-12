package io.digdag.core.agent;

import java.lang.management.ManagementFactory;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;

public class AgentConfigProvider
    implements Provider<AgentConfig>
{
    private final AgentConfig config;

    @Inject
    public AgentConfigProvider(ConfigElement ce, ConfigFactory cf)
    {
        this.config = AgentConfig.convertFrom(ce.toConfig(cf));
    }

    @Override
    public AgentConfig get()
    {
        return config;
    }
}

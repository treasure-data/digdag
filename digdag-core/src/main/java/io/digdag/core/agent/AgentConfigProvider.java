package io.digdag.core.agent;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;

public class AgentConfigProvider
    implements Provider<AgentConfig>
{
    private final AgentConfig config;

    @Inject
    public AgentConfigProvider(Config systemConfig)
    {
        this.config = AgentConfig.convertFrom(systemConfig);
    }

    @Override
    public AgentConfig get()
    {
        return config;
    }
}

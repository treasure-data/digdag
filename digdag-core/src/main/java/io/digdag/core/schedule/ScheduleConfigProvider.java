package io.digdag.core.schedule;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;

public class ScheduleConfigProvider
    implements Provider<ScheduleConfig>
{
    private final ScheduleConfig config;

    @Inject
    public ScheduleConfigProvider(Config systemConfig)
    {
        this.config = ScheduleConfig.convertFrom(systemConfig);
    }

    @Override
    public ScheduleConfig get()
    {
        return config;
    }
}

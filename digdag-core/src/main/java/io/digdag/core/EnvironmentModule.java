package io.digdag.core;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

import java.util.Map;

public class EnvironmentModule implements Module
{
    private final Map<String, String> environment;

    public EnvironmentModule(Map<String, String> environment)
    {

        this.environment = environment;
    }

    @Override
    public void configure(Binder binder)
    {

    }

    @Provides @Environment
    Map<String, String> provideEnvironment() {
        return environment;
    }
}

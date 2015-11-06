package io.digdag.server;

import com.google.inject.Injector;
import io.digdag.DigdagEmbed;
import io.digdag.guice.rs.GuiceRsBootstrap;

public class ServerBootstrap
        implements GuiceRsBootstrap
{
    @Override
    public Injector initialize()
    {
        return new DigdagEmbed.Bootstrap()
            .addModules(new ServerModule())
            .initialize()
            .getInjector();
    }
}

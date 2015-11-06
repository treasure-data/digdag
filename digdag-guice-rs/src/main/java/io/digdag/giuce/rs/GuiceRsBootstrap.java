package io.digdag.guice.rs;

import com.google.inject.Injector;

public interface GuiceRsBootstrap
{
    Injector initialize();
}

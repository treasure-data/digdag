package io.digdag.guice.rs;

import javax.servlet.ServletContext;
import com.google.inject.Injector;

public interface GuiceRsBootstrap
{
    Injector initialize(ServletContext context);
}

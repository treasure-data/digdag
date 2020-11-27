package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;
import io.digdag.spi.CommandLogger;

public class SimpleCommandExecutorFactory
        implements CommandExecutorFactory
{
    private final CommandLogger clog;

    @Inject
    public SimpleCommandExecutorFactory(CommandLogger clog)
    {
        this.clog = clog;
    }

    @Override
    public String getType()
    {
        return "simple";
    }

    @Override
    public CommandExecutor newCommandExecutor()
    {
        return new SimpleCommandExecutor(clog);
    }
}

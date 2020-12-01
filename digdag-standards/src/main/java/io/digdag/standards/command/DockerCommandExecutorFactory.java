package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;
import io.digdag.spi.CommandLogger;

public class DockerCommandExecutorFactory
        implements CommandExecutorFactory
{
    private final CommandLogger clog;
    private final SimpleCommandExecutor simple;

    @Inject
    public DockerCommandExecutorFactory(CommandLogger clog)
    {
        this.clog = clog;
        this.simple = new SimpleCommandExecutor(clog);
    }

    @Override
    public String getType()
    {
        return "docker";
    }

    @Override
    public CommandExecutor newCommandExecutor()
    {
        return new DockerCommandExecutor(clog, simple);
    }
}

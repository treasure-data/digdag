package io.digdag.standards.command;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandStatus;

public class ProcessCommandStatus
        implements CommandStatus
{
    static CommandStatus of(final int statusCode, final CommandExecutorContent outputContent)
    {
        return new ProcessCommandStatus(statusCode, outputContent);
    }

    private final Optional<Integer> statusCode;
    private final CommandExecutorContent outputContent;

    private ProcessCommandStatus(final int statusCode, final CommandExecutorContent outputContent)
    {
        this.statusCode = Optional.of(statusCode);
        this.outputContent = outputContent;
    }

    @Override
    public boolean isFinished()
            throws InterruptedException
    {
        return true;
    }

    @Override
    public Optional<Integer> getStatusCode()
    {
        return statusCode;

    }
    @Override
    public String getCommandId()
    {
        throw new UnsupportedOperationException("this method is never called.");
    }

    @Override
    public Config getExecutorState()
    {
        throw new UnsupportedOperationException("this method is never called.");
    }

    @Override
    public CommandExecutorContent getOutputContent()
    {
        return outputContent;
    }
}
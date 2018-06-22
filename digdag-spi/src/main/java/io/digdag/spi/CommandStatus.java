package io.digdag.spi;

import io.digdag.client.config.Config;

public interface CommandStatus
{
    boolean isFinished()
            throws InterruptedException;

    int getStatusCode();

    String getCommandId();

    Config getExecutorState(); // used for storing the command executor status like logger pagination

    CommandExecutorContent getOutputContent(); // {content}
}
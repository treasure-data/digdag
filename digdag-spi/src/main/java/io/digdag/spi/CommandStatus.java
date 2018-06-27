package io.digdag.spi;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;

public interface CommandStatus
{
    boolean isFinished()
            throws InterruptedException;

    Optional<Integer> getStatusCode();

    String getCommandId();

    Config getExecutorState(); // used for storing the command executor status like logger pagination

    CommandExecutorContent getOutputContent(); // {content}
}
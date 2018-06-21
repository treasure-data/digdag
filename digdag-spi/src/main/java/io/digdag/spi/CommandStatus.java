package io.digdag.spi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;

import java.io.IOException;

public interface CommandStatus
{
    boolean isFinished()
            throws InterruptedException;

    Optional<String> getCommandId();

    int getExitCode();

    Config getTaskResultData(ObjectMapper mapper, Class<Config> dataType)
            throws IOException;
}
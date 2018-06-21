package io.digdag.standards.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.CommandStatus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ProcessCommandStatus
        implements CommandStatus
{
    private final Process p;
    private final Path outFilePath;

    ProcessCommandStatus(final Process p, final Path outFilePath)
    {
        this.p = p;
        this.outFilePath = outFilePath;
    }

    @Override
    public boolean isFinished()
            throws InterruptedException
    {
        p.waitFor();
        return true;
    }

    @Override
    public Optional<String> getCommandId()
    {
        return Optional.absent();
    }

    @Override
    public int getExitCode()
    {
        return p.exitValue();
    }

    @Override
    public Config getTaskResultData(final ObjectMapper mapper, Class<Config> dataType)
            throws IOException
    {
        final File outFile = outFilePath.toFile();
        return mapper.readValue(outFile, dataType);
    }

}
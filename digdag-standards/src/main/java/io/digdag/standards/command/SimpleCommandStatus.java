package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.spi.CommandStatus;

import java.nio.file.Path;

class SimpleCommandStatus
        implements CommandStatus
{
    static CommandStatus of(final Process p, final Path ioDirectory)
    {
        return new SimpleCommandStatus(p.exitValue(), ioDirectory.toString());
    }

    private final int statusCode;
    private final String ioDirectory;

    private SimpleCommandStatus(final int statusCode, final String ioDirectory)
    {
        this.statusCode = statusCode;
        this.ioDirectory = ioDirectory;
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public int getStatusCode()
    {
        return statusCode;
    }

    @Override
    public String getIoDirectory()
    {
        return ioDirectory;
    }

    @Override
    public ObjectNode toJson()
    {
        throw new UnsupportedOperationException();
    }
}

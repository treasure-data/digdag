package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.spi.CommandStatus;

public class ProcessCommandStatus
        implements CommandStatus
{
    static CommandStatus of(final String ioDirectory, final Process p)
    {
        return new ProcessCommandStatus(p.exitValue(), ioDirectory);
    }

    private final int statusCode;
    private final String ioDirectory;

    private ProcessCommandStatus(final int statusCode, final String ioDirectory)
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

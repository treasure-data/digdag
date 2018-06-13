package io.digdag.standards.command;

import io.digdag.spi.CommandResult;

public class ProcessCommandResult
        implements CommandResult
{
    private final Process p;

    ProcessCommandResult(final Process p)
    {
        this.p = p;
    }

    @Override
    public boolean isFinished()
            throws InterruptedException
    {
        p.waitFor();
        return true;
    }

    @Override
    public int getExitCode()
    {
        return p.exitValue();
    }
}
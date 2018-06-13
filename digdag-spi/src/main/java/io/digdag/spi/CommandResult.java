package io.digdag.spi;

public interface CommandResult
{
    boolean isFinished()
            throws InterruptedException;

    int getExitCode();
}
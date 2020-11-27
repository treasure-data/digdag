package io.digdag.spi;

public interface CommandExecutorFactory
{
    String getType();

    CommandExecutor newCommandExecutor();
}

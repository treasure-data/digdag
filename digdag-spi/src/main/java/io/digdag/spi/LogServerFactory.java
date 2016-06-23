package io.digdag.spi;

public interface LogServerFactory
{
    public String getType();

    public LogServer getLogServer();
}

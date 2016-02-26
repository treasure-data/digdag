package io.digdag.spi;

import io.digdag.client.config.Config;

public interface LogServerFactory
{
    public String getType();

    public LogServer getLogServer(Config systemConfig);
}

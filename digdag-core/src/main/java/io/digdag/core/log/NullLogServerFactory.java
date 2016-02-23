package io.digdag.core.log;

import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;

public class NullLogServerFactory
    implements LogServerFactory
{
    @Override
    public LogServer getLogServer()
    {
        return new LogServer()
        {
            public void send(long taskId, byte[] data)
            { }

            public byte[] get(long taskId, int index)
            {
                return null;
            }
        };
    }
}

package io.digdag.core.log;

import com.google.inject.Inject;
import io.digdag.spi.LogClient;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import java.util.function.Consumer;

public class LogServerManager
{
    private final LogServerFactory factory;

    @Inject
    public LogServerManager(LogServerFactory factory)
    {
        this.factory = factory;
    }

    public LogServer getLogServer()
    {
        return factory.getLogServer();
    }

    public LogClient getInProcessLogClient(int siteId)
    {
        final LogServer server = getLogServer();
        return new LogClient()
        {
            public void send(long taskId, byte[] data)
            {
                server.send(taskId, data);
            }

            public void get(long taskId, Consumer<byte[]> consumer)
            {
                int index = 0;
                while (true) {
                    byte[] data = server.get(taskId, index);
                    if (data == null) {
                        return;
                    }
                    consumer.accept(data);
                }
            }
        };
    }
}

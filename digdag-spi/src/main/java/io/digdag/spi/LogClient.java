package io.digdag.spi;

import java.util.function.Consumer;

public interface LogClient
{
    void send(long taskId, byte[] data);

    void get(long taskId, Consumer<byte[]> consumer);
}

package io.digdag.spi.log;

public interface LogDirectClient
{
    void send(long taskId, byte[] data);

    byte[] get(long taskId, int index);
}

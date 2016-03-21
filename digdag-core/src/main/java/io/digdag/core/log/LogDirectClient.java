package io.digdag.core.log;

public interface LogDirectClient
{
    void send(long taskId, byte[] data);

    byte[] get(long taskId, int index);
}

package io.digdag.spi;

public interface ParamServerClientConnection<T>
{
    T get();

    String getType();
}

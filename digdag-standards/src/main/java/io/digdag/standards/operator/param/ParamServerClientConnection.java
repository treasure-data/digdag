package io.digdag.standards.operator.param;

public interface ParamServerClientConnection<T>
{
    T get();

    String getType();
}

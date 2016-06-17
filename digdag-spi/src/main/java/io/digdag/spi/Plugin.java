package io.digdag.spi;

import java.util.List;

public interface Plugin
{
    <T> Class<? extends T> getServiceProvider(Class<T> type);
}

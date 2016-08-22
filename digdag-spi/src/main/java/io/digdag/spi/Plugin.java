package io.digdag.spi;

import com.google.inject.Binder;

public interface Plugin
{
    <T> Class<? extends T> getServiceProvider(Class<T> type);

    default <T> void configureBinder(Class<T> type, Binder binder) {
    }
}

package io.digdag.spi;

import java.util.List;

public interface Plugin
{
    <T> List<T> get(Class<T> iface);
}

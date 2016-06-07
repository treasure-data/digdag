package io.digdag.core.plugin;

import java.util.function.Function;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Injector;
import io.digdag.spi.Plugin;

public class DynamicPluginLoader<R>
{
    private final PluginLoader loader;
    private final Injector injector;
    private final Function<PluginSet, R> cacheBuilder;
    private final Cache<Spec, R> cache;

    public DynamicPluginLoader(PluginLoader loader, Injector injector,
            int maxCacheSize, Function<PluginSet, R> cacheBuilder)
    {
        this.loader = loader;
        this.injector = injector;
        this.cacheBuilder = cacheBuilder;
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();
    }

    public R get(Spec spec)
    {
        try {
            return cache.get(spec, () -> loadCache(spec));
        }
        catch (ExecutionException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

    private R loadCache(Spec spec)
    {
        PluginSet plugins = loader.load(spec).create(injector);
        return cacheBuilder.apply(plugins);
    }
}

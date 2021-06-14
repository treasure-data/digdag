package io.digdag.core.plugin;

import java.util.function.Function;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Module;
import com.google.inject.Injector;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.digdag.commons.ThrowablesUtil;

public class DynamicPluginLoader<R>
{
    public static <R> DynamicPluginLoader<R> build(
            PluginLoader loader,
            Module restrictInjectModule,
            Function<PluginSet.WithInjector, R> cacheBuilder,
            int maxCacheSize)
    {
        return new DynamicPluginLoader<>(
                loader, restrictInjectModule,
                cacheBuilder, maxCacheSize);
    }

    private final PluginLoader loader;
    private final Injector injector;
    private final Function<PluginSet.WithInjector, R> cacheBuilder;
    private final Cache<Spec, R> cache;

    private DynamicPluginLoader(
            PluginLoader loader,
            Module restrictInjectModule,
            Function<PluginSet.WithInjector, R> cacheBuilder,
            int maxCacheSize)
    {
        this.loader = loader;
        this.injector = buildRestrictedInjector(restrictInjectModule);
        this.cacheBuilder = cacheBuilder;
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();
    }

    public R load(Spec spec)
    {
        try {
            return cache.get(spec, () -> loadCache(spec));
        }
        catch (UncheckedExecutionException ex) {
            throw ThrowablesUtil.propagate(ex.getCause());
        }
        catch (ExecutionException ex) {
            throw ThrowablesUtil.propagate(ex.getCause());
        }
    }

    private R loadCache(Spec spec)
    {
        PluginSet plugins = loader.load(spec);
        return cacheBuilder.apply(plugins.withInjector(injector));
    }

    private static Injector buildRestrictedInjector(Module module)
    {
        return Guice.createInjector(
                Stage.PRODUCTION,
                ImmutableList.of(module, (binder) -> {
                    binder.disableCircularProxies();
                }));
    }
}

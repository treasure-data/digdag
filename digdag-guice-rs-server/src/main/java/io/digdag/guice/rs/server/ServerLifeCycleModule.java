package io.digdag.guice.rs.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ServerLifeCycleModule
    implements Module
{
    // instances created before getServerManager() call
    private final List<Object> earlyInjected = new ArrayList<>();

    private final AtomicReference<ServerLifeCycleManager> lifeCycleManagerRef = new AtomicReference<ServerLifeCycleManager>(null);

    @Override
    public void configure(Binder binder)
    {
        binder.disableCircularProxies();

        binder.bindListener(Matchers.any(), new TypeListener()
        {
            @Override
            public <T> void hear(TypeLiteral<T> type, TypeEncounter<T> encounter)
            {
                encounter.register(new InjectionListener<T>()
                {
                    @Override
                    public void afterInjection(T obj)
                    {
                        ServerLifeCycleManager initialized = lifeCycleManagerRef.get();
                        if (initialized == null) {
                            earlyInjected.add(obj);
                        }
                        else {
                            try {
                                initialized.manageInstance(obj);
                            }
                            catch (Exception e) {
                                // really nothing we can do here
                                throw new Error(e);
                            }
                        }
                    }
                });
            }
        });
    }

    @Provides
    @Singleton
    public ServerLifeCycleManager getServerManager()
            throws Exception
    {
        ServerLifeCycleManager initialized = new ServerLifeCycleManager();
        lifeCycleManagerRef.set(initialized);
        for (Object delayed : earlyInjected) {
            initialized.manageInstance(delayed);
        }
        earlyInjected.clear();
        return initialized;
    }
}

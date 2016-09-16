package io.digdag.server;

import com.google.inject.Inject;
import org.weakref.jmx.Managed;
import io.digdag.core.ErrorReporter;
import java.util.concurrent.atomic.AtomicInteger;

public class JmxErrorReporter
    implements ErrorReporter
{
    private final AtomicInteger uncaughtErrorCount = new AtomicInteger(0);

    @Inject
    public JmxErrorReporter()
    { }

    @Override
    public void reportUncaughtError(Throwable error)
    {
        uncaughtErrorCount.incrementAndGet();
    }

    @Managed
    public int getUncaughtErrorCount()
    {
        return uncaughtErrorCount.get();
    }
}

package io.digdag.core.agent;

import static java.util.Locale.ENGLISH;

public class SetThreadName
    implements AutoCloseable
{
    private final Thread thread;
    private final String savedName;

    public SetThreadName(String name)
    {
        this(Thread.currentThread(), name);
    }

    public SetThreadName(Thread thread, String name)
    {
        this.thread = thread;
        this.savedName = thread.getName();
        thread.setName(String.format(ENGLISH, "%04d@", thread.getId()) + name);
    }

    @Override
    public void close()
    {
        thread.setName(savedName);
    }
}

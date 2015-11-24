package io.digdag.core.agent;

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
        thread.setName(name);
    }

    @Override
    public void close()
    {
        thread.setName(savedName);
    }
}

package io.digdag.core;

public interface ErrorReporter
{
    void reportUncaughtError(Throwable error);

    static ErrorReporter empty()
    {
        return new ErrorReporter()
        {
            @Override
            public void reportUncaughtError(Throwable error)
            { }
        };
    }
}

package io.digdag.core;

/**
 * ErrorReporter will be deprecated in v0.11.x
 */
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

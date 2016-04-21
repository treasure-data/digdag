package io.digdag.cli.client;

public class ShowAttempt
    extends ShowSession
{
    @Override
    protected boolean includeRetries()
    {
        return true;
    }
}

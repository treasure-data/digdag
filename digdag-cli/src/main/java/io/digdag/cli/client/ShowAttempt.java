package io.digdag.cli.client;

import java.io.PrintStream;

public class ShowAttempt
    extends ShowSession
{
    public ShowAttempt(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    protected boolean includeRetries()
    {
        return true;
    }
}

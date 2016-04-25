package io.digdag.cli.client;

import io.digdag.core.*;
import io.digdag.core.Version;

import java.io.PrintStream;

public class ShowAttempt
    extends ShowSession
{
    public ShowAttempt(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    protected boolean includeRetries()
    {
        return true;
    }
}

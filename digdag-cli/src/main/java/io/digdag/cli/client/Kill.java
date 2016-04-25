package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.core.*;
import io.digdag.core.Version;

import java.io.PrintStream;

import static io.digdag.cli.SystemExitException.systemExit;

public class Kill
    extends ClientCommand
{
    public Kill(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        kill(parseLongOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag kill <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void kill(long attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();
        client.killSessionAttempt(attemptId);
        ln("Kill requirested session attempt " + attemptId);
    }
}

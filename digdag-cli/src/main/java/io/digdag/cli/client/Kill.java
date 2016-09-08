package io.digdag.cli.client;

import io.digdag.cli.CommandContext;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;

import static io.digdag.cli.SystemExitException.systemExit;

public class Kill
    extends ClientCommand
{
    public Kill(CommandContext context)
    {
        super(context);
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
        ctx.err().println("Usage: " + ctx.programName() + " kill <attempt-id>");
        ctx.err().println("  Options:");
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

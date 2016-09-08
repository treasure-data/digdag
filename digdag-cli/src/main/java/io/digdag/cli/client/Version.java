package io.digdag.cli.client;

import io.digdag.cli.CommandContext;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;

import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class Version extends ClientCommand
{
    public Version(CommandContext context)
    {
        super(context);
    }

    @Override
    public void mainWithClientException()
            throws Exception
    {
        DigdagClient client = buildClient(false);
        Map<String, Object> remoteVersion = client.getVersion();
        ln("Client version: " + ctx.version());
        ln("Server version: " + remoteVersion.getOrDefault("version", ""));
    }

    @Override
    public SystemExitException usage(String error)
    {
        ctx.err().println("Usage: " + ctx.programName() + " version");
        ctx.err().println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }
}

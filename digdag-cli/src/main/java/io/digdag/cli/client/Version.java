package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;

import java.io.PrintStream;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class Version extends ClientCommand
{
    public Version(io.digdag.core.Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
            throws Exception
    {
        DigdagClient client = buildClient();
        Map<String, Object> remoteVersion = client.getVersion();
        ln("Client version: " + localVersion);
        ln("Server version: " + remoteVersion.getOrDefault("version", ""));
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag version");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }
}

package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;

import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class Version extends ClientCommand
{
    @Override
    public void mainWithClientException()
            throws Exception
    {
        ln("Client version: " + version);
        try {
            DigdagClient client = buildClient(false);
            Map<String, Object> remoteVersion = client.getVersion();
            ln("Server version: " + remoteVersion.getOrDefault("version", ""));
        }
        catch (Exception e) {
            ln("An error happened during getting Server version.");
            throw e;
        }
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " version");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }
}

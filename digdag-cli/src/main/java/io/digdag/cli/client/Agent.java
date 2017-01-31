package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.cli.SystemExitException.systemExit;

public class Agent
    extends ClientCommand
{
    // TODO --pid-file

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }

        Path workingDir = Paths.get(args.get(0));
        agent(workingDir);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " agent <working-dir>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void agent(Path workingDir)
        throws Exception
    {
        DigdagClient client = buildClient();

        AgentRunner runner = new AgentRunner(client, workingDir);
        runner.run();
    }
}

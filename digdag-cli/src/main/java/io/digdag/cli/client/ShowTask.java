package io.digdag.cli.client;

import java.util.List;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestTask;
import static io.digdag.cli.Main.systemExit;

public class ShowTask
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        showTasks(parseLongOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag tasks <session-id>");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showTasks(long sessionId)
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestTask> tasks = client.getTasks(sessionId);
        modelPrinter().printList(tasks);
    }
}

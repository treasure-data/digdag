package io.digdag.cli.client;

import java.util.List;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSession;
import static io.digdag.cli.Main.systemExit;

public class ShowSession
    extends ClientCommand
{
    @Override
    public void main()
        throws Exception
    {
        if (args.isEmpty()) {
            showSessions();
        }
        else {
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sessions [id]");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showSessions()
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSession> sessions = client.getSessions();
        modelPrinter().printList(sessions);
    }
}

package io.digdag.cli.client;

import java.util.List;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import static io.digdag.cli.Main.systemExit;

public class ShowSchedule
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.isEmpty()) {
            showSchedules();
        }
        else {
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag schedules");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showSchedules()
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSchedule> schedules = client.getSchedules();
        modelPrinter().printList(schedules);
    }
}

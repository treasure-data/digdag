package io.digdag.cli.client;

import java.io.PrintStream;
import java.time.Instant;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import io.digdag.core.Version;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowSchedule
    extends ClientCommand
{
    public ShowSchedule(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 0:
            showSchedules();
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag schedules");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void showSchedules()
        throws Exception
    {
        Instant now = Instant.now();

        DigdagClient client = buildClient();
        ln("Schedules:");
        int count = 0;
        for (RestSchedule sched : client.getSchedules()) {
            ln("  id: %d", sched.getId());
            ln("  project: %s", sched.getProject().getName());
            ln("  workflow: %s", sched.getWorkflow().getName());
            ln("  next session time: %s", TimeUtil.formatTime(sched.getNextScheduleTime()));
            ln("  next runs at: %s (%s later)", TimeUtil.formatTime(sched.getNextRunTime()), TimeUtil.formatTimeDiff(now, sched.getNextRunTime()));
            ln("");
            count++;
        }
        ln("%d entries.", count);
        err.println("Use `digdag workflows [project-name] [workflow-name]` to show workflow details.");
    }
}

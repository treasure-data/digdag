package io.digdag.cli.client;

import com.google.common.base.Optional;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;

import java.time.Instant;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.TimeUtil.formatTime;
import static io.digdag.cli.TimeUtil.formatTimeWithDiff;

public class ShowSchedule
    extends ClientCommand
{
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
        err.println("Usage: " + programName + " schedules");
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
        for (RestSchedule sched : client.getSchedules(Optional.absent()).getSchedules()) {  // TODO use pagination (last_id) to get all schedules
            ln("  id: %s", sched.getId());
            ln("  project: %s", sched.getProject().getName());
            ln("  workflow: %s", sched.getWorkflow().getName());
            ln("  disabled at: " + sched.getDisabledAt().transform(ts -> formatTimeWithDiff(now, ts)).or(""));
            ln("  start date : " + sched.getStartDate().transform(ts -> formatTimeWithDiff(now, ts)).or(""));
            ln("  end date: " + sched.getEndDate().transform(ts -> formatTimeWithDiff(now, ts)).or(""));
            ln("  next session time: %s", formatTime(sched.getNextScheduleTime()));
            ln("  next scheduled to run at: %s", formatTimeWithDiff(now, sched.getNextRunTime()));
            ln("");
            count++;
        }
        ln("%d entries.", count);
        err.println("Use `" + programName + " workflows [project-name] [name]` to show workflow details.");
    }
}

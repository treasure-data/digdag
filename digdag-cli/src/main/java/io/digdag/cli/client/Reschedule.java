package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;

import java.time.Instant;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.TimeUtil.formatTimeWithDiff;

public class Reschedule
    extends ClientCommand
{
    @Parameter(names = {"-s", "--skip"})
    int skipCount = 0;

    @Parameter(names = {"-t", "--skip-to"})
    String toTime = null;

    @Parameter(names = {"-a", "--run-at"})
    String runAtTime = null;

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (toTime != null && skipCount > 0) {
            throw systemExit("-s and -t can't be set together");
        }
        else if (toTime == null && skipCount <= 0) {
            throw usage("-s or -t is required");
        }

        // Schedule id?
        if (args.size() == 1) {
            Id schedId = parseScheduleId(args.get(0));
            rescheduleScheduleId(schedId);
        }
        else if (args.size() == 2) {
            // Single workflow
            rescheduleWorkflow(args.get(0), args.get(1));
        }
        else {
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " reschedule <schedule-id> | <project-name> <name>");
        err.println("  Options:");
        err.println("    -s, --skip N                     skips specified number of schedules from now");
        err.println("    -t, --skip-to 'yyyy-MM-dd HH:mm:ss Z' | 'now'");
        err.println("                                     skips schedules until the specified time (exclusive)");
        err.println("    -a, --run-at 'yyyy-MM-dd HH:mm:ss Z'");
        err.println("                                     set next run time to this time");
        err.println("    -d, --dry-run                    tries to reschedule and validates the results but does nothing");
        showCommonOptions();
        return systemExit(error);
    }

    private Id parseScheduleId(String s)
            throws SystemExitException
    {
        try {
            return Id.of(Integer.toString(Integer.parseUnsignedInt(s)));
        }
        catch (NumberFormatException ignore) {
            throw usage(null);
        }
    }

    private void rescheduleWorkflow(String projectName, String workflowName)
            throws Exception
    {
        DigdagClient client = buildClient();
        RestProject project = client.getProject(projectName);
        RestSchedule schedule = client.getSchedule(project.getId(), workflowName);
        Instant now = Instant.now();
        reschedule(schedule.getId(), client, now);
    }

    private void rescheduleScheduleId(Id schedId)
        throws Exception
    {
        DigdagClient client = buildClient();
        Instant now = Instant.now();
        reschedule(schedId, client, now);
    }

    private void reschedule(Id schedId, DigdagClient client, Instant now)
        throws Exception
    {
        Optional<Instant> runAt = runAtTime == null ? Optional.absent() : Optional.of(
                TimeUtil.parseTime(runAtTime, "-a, --run-at")
                );

        RestScheduleSummary updated;
        if (toTime != null) {
            Instant time = "now".equals(toTime) ?
                now :
                TimeUtil.parseTime(toTime, "-t, --skip-to");
            updated = client.skipSchedulesToTime(schedId,
                    time,
                    runAt,
                    dryRun);
        }
        else {
            updated = client.skipSchedulesByCount(schedId,
                    now, skipCount,
                    runAt,
                    dryRun);
        }

        ln("  id: %s", updated.getId());
        ln("  workflow: %s", updated.getWorkflow().getName());
        ln("  disabled at: " + updated.getDisabledAt().transform(ts -> formatTimeWithDiff(now, ts)).or(""));
        ln("  next session time: %s", TimeUtil.formatTime(updated.getNextScheduleTime()));
        ln("  next scheduled to run at: %s", TimeUtil.formatTimeWithDiff(now, updated.getNextRunTime()));
        ln("");

        if (dryRun) {
            err.println("Schedule is not updated.");
        }
        else {
            err.println("Use `" + programName + " schedules` to show schedules.");
        }
    }
}

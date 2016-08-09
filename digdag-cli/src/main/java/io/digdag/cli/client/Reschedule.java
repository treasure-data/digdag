package io.digdag.cli.client;

import java.io.PrintStream;
import java.time.Instant;

import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.core.Version;

import static io.digdag.cli.SystemExitException.systemExit;

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

    public Reschedule(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        int schedId = parseIntOrUsage(args.get(0));

        if (toTime != null && skipCount > 0) {
            throw systemExit("-s and -t can't be set together");
        }
        else if (toTime == null && skipCount <= 0) {
            throw usage("-s or -t is required");
        }
        reschedule(schedId);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag reschedule <schedule-id>");
        err.println("  Options:");
        err.println("    -s, --skip N                     skips specified number of schedules from now");
        err.println("    -t, --skip-to 'yyyy-MM-dd HH:mm:ss Z'  skips schedules until the specified time (exclusive)");
        err.println("    -a, --run-at 'yyyy-MM-dd HH:mm:ss Z'   set next run time to this time");
        err.println("    -d, --dry-run                    tries to reschedule and validates the results but does nothing");
        showCommonOptions();
        return systemExit(error);
    }

    private void reschedule(int schedId)
        throws Exception
    {
        Instant now = Instant.now();

        Optional<Instant> runAt = runAtTime == null ? Optional.absent() : Optional.of(
                TimeUtil.parseTime(runAtTime, "-a, --run-at")
                );

        DigdagClient client = buildClient();
        RestScheduleSummary updated;
        if (toTime != null) {
            updated = client.skipSchedulesToTime(schedId,
                    TimeUtil.parseTime(toTime, "-t, --skip-to"),
                    runAt,
                    dryRun);
        }
        else {
            updated = client.skipSchedulesByCount(schedId,
                    now, skipCount,
                    runAt,
                    dryRun);
        }

        ln("  id: %d", updated.getId());
        ln("  workflow: %s", updated.getWorkflow().getName());
        ln("  next session time: %s", TimeUtil.formatTime(updated.getNextScheduleTime()));
        ln("  next runs at: %s (%s later)", TimeUtil.formatTime(updated.getNextRunTime()), TimeUtil.formatTimeDiff(now, updated.getNextRunTime()));
        ln("");

        if (dryRun) {
            err.println("Schedule is not updated.");
        }
        else {
            err.println("Use `digdag schedules` to show schedules.");
        }
    }
}

package io.digdag.cli.client;

import java.util.Date;
import java.util.List;
import java.time.Instant;
import java.io.File;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestScheduleSkipRequest;
import static io.digdag.cli.Main.systemExit;

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
        if (args.size() != 1) {
            throw usage(null);
        }
        long schedId = parseLongOrUsage(args.get(0));

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
        System.err.println("Usage: digdag reschedule <schedule-id>");
        System.err.println("  Options:");
        System.err.println("    -s, --skip N                     skips specified number of schedules from now");
        System.err.println("    -t, --skip-to 'yyyy-MM-dd HH:mm:ss Z'  skips schedules until the specified time (exclusive)");
        System.err.println("    -a, --run-at 'yyyy-MM-dd HH:mm:ss Z'   set next run time to this time");
        System.err.println("    -d, --dry-run                    tries to reschedule and validates the results but does nothing");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void reschedule(long schedId)
        throws Exception
    {
        Instant now = Instant.now();

        DigdagClient client = buildClient();
        RestScheduleSummary updated;
        if (toTime != null) {
            updated = client.skipSchedulesToTime(schedId,
                    parseDate(toTime),
                    Optional.fromNullable(runAtTime).transform(t -> parseDate(t)),
                    dryRun);
        }
        else {
            updated = client.skipSchedulesByCount(schedId,
                    Date.from(now), skipCount,
                    Optional.fromNullable(runAtTime).transform(t -> parseDate(t)),
                    dryRun);
        }

        ln("  id: %d", updated.getId());
        ln("  workflow: %s", updated.getWorkflowName());
        ln("  next session time: %s", formatTime(updated.getNextScheduleTime()));
        ln("  next runs at: %s (%s later)", formatTime(updated.getNextRunTime()), formatTimeDiff(now, updated.getNextRunTime()));
        ln("");

        if (dryRun) {
            System.err.println("Schedule is not updated.");
        }
        else {
            System.err.println("Use `digdag schedules` to show schedules.");
        }
    }

    private Date parseDate(String s)
    {
        return Date.from(parseTime(s));
    }
}

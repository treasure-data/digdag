package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.List;
import java.time.Instant;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import static io.digdag.cli.SystemExitException.systemExit;

public class Backfill
    extends ClientCommand
{
    @Parameter(names = {"-f", "--from"})
    String fromTime;

    @Parameter(names = {"--attempt-name"})
    String attemptName;

    // TODO -n for count
    // TODO -t for to-time

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    public Backfill(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        int schedId = parseIntOrUsage(args.get(0));

        backfill(schedId);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag backfill <schedule-id>");
        err.println("  Options:");
        err.println("    -f, --from 'yyyy-MM-dd HH:mm:ss Z'  timestamp to start backfill from (required)");
        err.println("    -R, --attempt-name NAME          retry attempt name (required)");
        err.println("    -d, --dry-run                    tries to backfill and validates the results but does nothing");
        showCommonOptions();
        return systemExit(error);
    }

    public void backfill(int schedId)
        throws Exception
    {
        if (fromTime == null || attemptName == null) {
            throw new ParameterException("-f, --from option and -R, --attempt-name option are required");
        }

        Instant from = parseTime(fromTime,
            "-f, --from option must be \"yyyy-MM-dd HH:mm:ss Z\" format or UNIX timestamp");

        DigdagClient client = buildClient();
        List<RestSessionAttempt> attempts = client.backfillSchedule(schedId, from, attemptName, dryRun);

        ln("Session attempts:");
        for (RestSessionAttempt attempt : attempts) {
            ln("  id: %d", attempt.getId());
            ln("  uuid: %s", attempt.getSessionUuid());
            ln("  project: %s", attempt.getProject().getName());
            ln("  workflow: %s", attempt.getWorkflow().getName());
            ln("  session time: %s", formatTime(attempt.getSessionTime()));
            ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
            ln("  params: %s", attempt.getParams());
            ln("  created at: %s", formatTime(attempt.getCreatedAt()));
            ln("");
        }

        if (dryRun || attempts.isEmpty()) {
            err.println("No session attempts started.");
        }
        else {
            err.println("Backfill session attempts started.");
            err.println("Use `digdag sessions` to show the session attempts.");
        }
    }
}

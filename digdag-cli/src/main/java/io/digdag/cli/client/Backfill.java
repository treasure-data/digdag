package io.digdag.cli.client;

import java.util.Date;
import java.util.List;
import java.time.Instant;
import java.io.File;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import static io.digdag.cli.Main.systemExit;

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
        System.err.println("Usage: digdag backfill <schedule-id>");
        System.err.println("  Options:");
        System.err.println("    -f, --from 'yyyy-MM-dd HH:mm:ss Z'  timestamp to start backfill from (required)");
        System.err.println("    -R, --attempt-name NAME          retry attempt name (required)");
        System.err.println("    -d, --dry-run                    tries to backfill and validates the results but does nothing");
        ClientCommand.showCommonOptions();
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
            System.err.println("No session attempts started.");
        }
        else {
            System.err.println("Backfill session attempts started.");
            System.err.println("Use `digdag sessions` to show the session attempts.");
        }
    }
}

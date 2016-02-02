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
import io.digdag.client.api.RestSession;
import static io.digdag.cli.Main.systemExit;

public class Backfill
    extends ClientCommand
{
    @Parameter(names = {"-f", "--from"}, required = true)
    String fromTime;

    @Parameter(names = {"-R", "--attempt-name"}, required = true)
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
        long schedId = parseLongOrUsage(args.get(0));

        backfill(schedId);
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag backfill <schedule-id>");
        System.err.println("  Options:");
        System.err.println("    -f, --from 'yyyy-MM-dd HH:mm:ss Z'  timestamp to start backfill from (required)");
        System.err.println("    -R, --attempt-name NAME          session attempt name (required)");
        System.err.println("    -d, --dry-run                    tries to backfill and validates the results but does nothing");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void backfill(long schedId)
        throws Exception
    {
        Date from = Date.from(parseTime(fromTime));

        DigdagClient client = buildClient();
        List<RestSession> sessions = client.backfillSchedule(schedId, from, attemptName, dryRun);

        ln("Sessions:");
        for (RestSession session : sessions) {
            ln("  id: %d", session.getId());
            ln("  repository: %s", session.getRepository().getName());
            ln("  workflow: %s", session.getWorkflowName());
            ln("  session time: %s", formatTime(session.getSessionTime()));
            ln("  retry attempt name: %s", session.getRetryAttemptName().or(""));
            ln("  params: %s", session.getParams());
            ln("  created at: %s", formatTime(session.getId()));
            ln("");
        }

        if (dryRun || sessions.isEmpty()) {
            System.err.println("No sessions started.");
        }
        else {
            System.err.println("Backfill sessions started.");
            System.err.println("Use `digdag sessions` to show the sessions.");
        }
    }
}

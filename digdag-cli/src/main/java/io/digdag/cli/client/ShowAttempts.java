package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.core.Version;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowAttempts
    extends ClientCommand
{
    @Parameter(names = {"-i", "--last-id"})
    Long lastId = null;

    public ShowAttempts(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
    }

    @Override
    public void mainWithClientException()
            throws Exception
    {
        switch (args.size()) {
            case 0:
                showAttempts(null);
                break;
            case 1:
                try {
                    long sessionId = Long.parseUnsignedLong(args.get(0));
                    showAttempts(sessionId);
                } catch (NumberFormatException ignore) {
                    throw usage("Invalid session id: " + args.get(0));
                }
                break;
            default:
                throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag attempts                         show attempts for all sessions");
        err.println("       digdag attempts <session-id>            show attempts for a session");
        err.println("  Options:");
        err.println("    -i, --last-id ID                 shows more session attempts from this id");
        showCommonOptions();
        return systemExit(error);
    }

    private void showAttempts(Long sessionId)
            throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSessionAttempt> attempts;

        if (sessionId == null) {
            attempts = client.getSessionAttempts(Optional.fromNullable(lastId));
        } else {
            attempts = client.getSessionAttempts(sessionId, Optional.fromNullable(lastId));
        }

        ln("Session attempts:");

        for (RestSessionAttempt attempt : Lists.reverse(attempts)) {
            printAttempt(attempt);
        }

        if (attempts.isEmpty()) {
            err.println("Use `digdag start` to start a session.");
        }
    }

    private void printAttempt(RestSessionAttempt attempt) {
        String status;
        if (attempt.getSuccess()) {
            status = "success";
        }
        else if (attempt.getDone()) {
            status = "error";
        }
        else {
            status = "running";
        }
        ln("  session id: %d", attempt.getSessionId());
        ln("  attempt id: %d", attempt.getId());
        ln("  uuid: %s", attempt.getSessionUuid());
        ln("  project: %s", attempt.getProject().getName());
        ln("  workflow: %s", attempt.getWorkflow().getName());
        ln("  session time: %s", TimeUtil.formatTime(attempt.getSessionTime()));
        ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
        ln("  params: %s", attempt.getParams());
        ln("  created at: %s", TimeUtil.formatTime(attempt.getCreatedAt()));
        ln("  finished at: %s", attempt.getFinishedAt().transform(TimeUtil::formatTime).or(""));
        ln("  kill requested: %s", attempt.getCancelRequested());
        ln("  status: %s", status);
        ln("");
    }
}

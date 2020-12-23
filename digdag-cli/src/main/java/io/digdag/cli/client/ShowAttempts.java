package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;

import java.util.List;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowAttempts
    extends ClientCommand
{
    @Parameter(names = {"-i", "--last-id"})
    Id lastId = null;

    @Parameter(names = {"-s", "--page-size"})
    Integer pageSize = null;

    @Override
    public void mainWithClientException()
            throws Exception
    {
        switch (args.size()) {
            case 0:
                showAttempts(null);
                break;
            case 1:
                Id sessionId = parseSessionIdOrUsage(args.get(0));
                showAttempts(sessionId);
                break;
            default:
                throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " attempts                         show attempts for all sessions");
        err.println("       " + programName + " attempts <session-id>            show attempts for a session");
        err.println("  Options:");
        err.println("    -i, --last-id ID                 shows more session attempts from this id");
        err.println("    -s, --page-size Number           shows more session attempts of the number of this page size (in default up to 100)");
        showCommonOptions();
        return systemExit(error);
    }

    private void showAttempts(Id sessionId)
            throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSessionAttempt> attempts;

        if (sessionId == null) {
            attempts = client.getSessionAttempts(Optional.fromNullable(lastId), Optional.fromNullable(pageSize)).getAttempts();
        } else {
            attempts = client.getSessionAttempts(sessionId, Optional.fromNullable(lastId), Optional.fromNullable(pageSize)).getAttempts();
        }

        ln("Session attempts:");

        for (RestSessionAttempt attempt : Lists.reverse(attempts)) {
            printAttempt(attempt);
        }

        if (attempts.isEmpty()) {
            err.println("Use `" + programName + " start` to start a session.");
        }
    }

    private void printAttempt(RestSessionAttempt attempt) {
        ln("  session id: %s", attempt.getSessionId());
        ln("  attempt id: %s", attempt.getId());
        ln("  uuid: %s", attempt.getSessionUuid());
        ln("  project: %s", attempt.getProject().getName());
        ln("  workflow: %s", attempt.getWorkflow().getName());
        ln("  session time: %s", TimeUtil.formatTime(attempt.getSessionTime()));
        ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
        ln("  params: %s", attempt.getParams());
        ln("  created at: %s", TimeUtil.formatTime(attempt.getCreatedAt()));
        ln("  finished at: %s", attempt.getFinishedAt().transform(TimeUtil::formatTime).or(""));
        ln("  kill requested: %s", attempt.getCancelRequested());
        ln("  status: %s", attempt.getStatus());
        ln("");
    }
}

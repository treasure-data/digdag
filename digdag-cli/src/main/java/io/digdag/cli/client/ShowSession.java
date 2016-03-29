package io.digdag.cli.client;

import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import static io.digdag.cli.Main.systemExit;

public class ShowSession
    extends ClientCommand
{
    @Parameter(names = {"-i", "--last-id"})
    Long lastId = null;

    @Parameter(names = {"-w", "--with-retry"})
    boolean withRetry = false;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 0:
            showSessions(null, null);
            break;
        case 1:
            showSessions(args.get(0), null);
            break;
        case 2:
            showSessions(args.get(0), args.get(1));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sessions [repo-name] [+name]");
        System.err.println("  Options:");
        System.err.println("    -i, --last-id ID                 shows more session attempts from this id");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showSessions(String repoName, String workflowName)
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSessionAttempt> attempts;

        if (repoName == null) {
            attempts = client.getSessionAttempts(withRetry, Optional.fromNullable(lastId));
        }
        else if (workflowName == null) {
            attempts = client.getSessionAttempts(repoName, withRetry, Optional.fromNullable(lastId));
        }
        else {
            attempts = client.getSessionAttempts(repoName, workflowName, withRetry, Optional.fromNullable(lastId));
        }

        ln("Session attempts:");
        for (RestSessionAttempt attempt : Lists.reverse(attempts)) {
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
            ln("  id: %d", attempt.getId());
            ln("  uuid: %s", attempt.getSessionUuid());
            ln("  repository: %s", attempt.getRepository().getName());
            ln("  workflow: %s", attempt.getWorkflowName());
            ln("  session time: %s", formatTime(attempt.getSessionTime()));
            ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
            ln("  params: %s", attempt.getParams());
            ln("  created at: %s", formatTime(attempt.getCreatedAt()));
            ln("  kill requested: %s", attempt.getCancelRequested());
            ln("  status: %s", status);
            ln("");
        }

        if (attempts.isEmpty()) {
            System.err.println("Use `digdag start` to start a session.");
        }
        else if (withRetry == false) {
            System.err.println("Use --with-retry option to show retried attempts.");
        }
    }
}

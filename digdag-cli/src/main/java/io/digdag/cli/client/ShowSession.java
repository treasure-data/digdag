package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import static io.digdag.cli.SystemExitException.systemExit;

public class ShowSession
    extends ClientCommand
{
    @Parameter(names = {"-i", "--last-id"})
    Long lastId = null;

    public ShowSession(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    // ShowAttempt overrides this method
    protected boolean includeRetries()
    {
        return false;
    }

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
        String commandName = includeRetries() ? "attempts" : "sessions";
        err.println("Usage: digdag " + commandName + " [project-name] [workflow-name]");
        err.println("  Options:");
        err.println("    -i, --last-id ID                 shows more session attempts from this id");
        showCommonOptions();
        return systemExit(error);
    }

    public void showSessions(String projName, String workflowName)
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSessionAttempt> attempts;

        if (projName == null) {
            attempts = client.getSessionAttempts(includeRetries(), Optional.fromNullable(lastId));
        }
        else if (workflowName == null) {
            attempts = client.getSessionAttempts(projName, includeRetries(), Optional.fromNullable(lastId));
        }
        else {
            attempts = client.getSessionAttempts(projName, workflowName, includeRetries(), Optional.fromNullable(lastId));
        }

        if (includeRetries()) {
            ln("Sessions:");
        }
        else {
            ln("Session attempts:");
        }
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
            ln("  attempt id: %d", attempt.getId());
            ln("  uuid: %s", attempt.getSessionUuid());
            ln("  project: %s", attempt.getProject().getName());
            ln("  workflow: %s", attempt.getWorkflow().getName());
            ln("  session time: %s", formatTime(attempt.getSessionTime()));
            ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
            ln("  params: %s", attempt.getParams());
            ln("  created at: %s", formatTime(attempt.getCreatedAt()));
            ln("  kill requested: %s", attempt.getCancelRequested());
            ln("  status: %s", status);
            ln("");
        }

        if (attempts.isEmpty()) {
            err.println("Use `digdag start` to start a session.");
        }
        else if (includeRetries() == false) {
            err.println("Use `digdag attempts` to show retried attempts.");
        }
    }
}

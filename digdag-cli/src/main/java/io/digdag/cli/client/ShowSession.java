package io.digdag.cli.client;

import java.util.List;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSession;
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
        System.err.println("    -i, --last-id ID                 shows more sessions from this id");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showSessions(String repoName, String workflowName)
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSession> sessions;

        if (repoName == null) {
            sessions = client.getSessions(withRetry, Optional.fromNullable(lastId));
        }
        else if (workflowName == null) {
            sessions = client.getSessions(repoName, withRetry, Optional.fromNullable(lastId));
        }
        else {
            sessions = client.getSessions(repoName, workflowName, withRetry, Optional.fromNullable(lastId));
        }

        ln("Sessions:");
        for (RestSession session : sessions) {
            String status;
            if (session.getSuccess()) {
                status = "success";
            }
            else if (session.getDone()) {
                status = "error";
            }
            else {
                status = "running";
            }
            ln("  id: %d", session.getId());
            ln("  repository: %s", session.getRepository().getName());
            ln("  workflow: %s", session.getWorkflowName());
            ln("  session time: %s", formatTime(session.getSessionTime()));
            ln("  retry attempt name: %s", session.getRetryAttemptName().or(""));
            ln("  params: %s", session.getParams());
            ln("  created at: %s", formatTime(session.getId()));
            ln("  kill requested: %s", session.getCancelRequested());
            ln("  status: %s", status);
            ln("");
        }

        if (sessions.isEmpty()) {
            System.err.println("Use `digdag start` to start a session.");
        }
        else if (withRetry == false) {
            System.err.println("Use --with-retry option to show retried sessions.");
        }
    }
}

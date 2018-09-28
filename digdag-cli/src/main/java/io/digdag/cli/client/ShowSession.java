package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSession;

import java.util.List;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowSession
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
                showSessions(null, null);
                break;
            case 1:
                Id sessionId = tryParseSessionId(args.get(0));
                if (sessionId == null) {
                    // Not a session id
                    showSessions(args.get(0), null);
                }
                else {
                    showSession(sessionId);
                }
                break;
            case 2:
                showSessions(args.get(0), args.get(1));
                break;
            default:
                throw usage(null);
        }
    }

    private static Id tryParseSessionId(String s)
    {
        try {
            return Id.of(Long.toString(Long.parseUnsignedLong(s)));
        }
        catch (NumberFormatException ignore) {
            return null;
        }
    }

    private void showSession(Id sessionId)
            throws Exception
    {
        DigdagClient client = buildClient();

        RestSession session = client.getSession(sessionId);
        if (session == null) {
            throw systemExit("Session with id " + sessionId + " not found.");
        }

        printSession(session);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " sessions                         show sessions for all workflows");
        err.println("       " + programName + " sessions <project-name>          show sessions for all workflows in a project");
        err.println("       " + programName + " sessions <project-name> <name>   show sessions for a workflow");
        err.println("       " + programName + " session  <session-id>            show a single session");
        err.println("  Options:");
        err.println("    -i, --last-id ID                 shows more sessions from this id");
        err.println("    -s, --page-size Number           shows more sessions of the number of this page size(in default up to 100)");
        showCommonOptions();
        return systemExit(error);
    }

    private void showSessions(String projName, String workflowName)
            throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSession> sessions;

        if (projName == null) {
            sessions = client.getSessions(Optional.fromNullable(lastId)).getSessions();
        } else {
            RestProject project = client.getProject(projName);
            if (workflowName == null) {
                sessions = client.getSessions(project.getId(), Optional.fromNullable(lastId), Optional.fromNullable(pageSize)).getSessions();
            }
            else {
                sessions = client.getSessions(project.getId(), workflowName, Optional.fromNullable(lastId), Optional.fromNullable(pageSize)).getSessions();
            }
        }

        ln("Sessions:");

        for (RestSession session : Lists.reverse(sessions)) {
            printSession(session);
        }

        if (sessions.isEmpty()) {
            err.println("Use `" + programName + " start` to start a session.");
        }
    }

    private void printSession(RestSession session)
    {
        ln("  session id: %s", session.getId());
        ln("  attempt id: %s", session.getLastAttempt().transform(a -> String.valueOf(a.getId())).or(""));
        ln("  uuid: %s", session.getSessionUuid());
        ln("  project: %s", session.getProject().getName());
        ln("  workflow: %s", session.getWorkflow().getName());
        ln("  session time: %s", TimeUtil.formatTime(session.getSessionTime()));
        ln("  retry attempt name: %s", session.getLastAttempt().transform(a -> a.getRetryAttemptName().or("")).or(""));
        ln("  params: %s", session.getLastAttempt().transform(a -> a.getParams().toString()).or(""));
        ln("  created at: %s", session.getLastAttempt().transform(a -> TimeUtil.formatTime(a.getCreatedAt())).or(""));
        ln("  kill requested: %s", session.getLastAttempt().transform(a -> a.getCancelRequested()).or(false));
        ln("  status: %s", status(session));
        ln("");
    }

    private String status(RestSession session)
    {
        return session.getLastAttempt().transform(a -> {
            if (a.getSuccess()) {
                return "success";
            }
            else if (a.getDone()) {
                return "error";
            }
            else {
                return "running";
            }
        }).or("pending");
    }
}

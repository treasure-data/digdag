package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.digdag.cli.EntityCollectionPrinter;
import io.digdag.cli.EntityPrinter;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSession;
import io.digdag.core.Version;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowSession
        extends ClientCommand
{
    @Parameter(names = {"-i", "--last-id"})
    Long lastId = null;

    public ShowSession(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
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
                try {
                    long sessionId = Long.parseUnsignedLong(args.get(0));
                    showSession(sessionId);
                    break;
                }
                catch (NumberFormatException ignore) {
                    // Not an attempt id
                }
                showSessions(args.get(0), null);
                break;
            case 2:
                showSessions(args.get(0), args.get(1));
                break;
            default:
                throw usage(null);
        }
    }

    private void showSession(long sessionId)
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
        err.println("Usage: digdag sessions                         show sessions for all workflows");
        err.println("       digdag sessions <project-name>          show sessions for all workflows in a project");
        err.println("       digdag sessions <project-name> <name>   show sessions for a workflow");
        err.println("       digdag session  <session-id>            show a single session");
        err.println("  Options:");
        err.println("    -i, --last-id ID                 shows more session attempts from this id");
        showCommonOptions();
        return systemExit(error);
    }

    private void showSessions(String projName, String workflowName)
            throws Exception
    {
        DigdagClient client = buildClient();
        List<RestSession> sessions;

        if (projName == null) {
            sessions = client.getSessions(Optional.fromNullable(lastId));
        } else {
            RestProject project = client.getProject(projName);
            if (workflowName == null) {
                sessions = client.getSessions(project.getId(), Optional.fromNullable(lastId));
            }
            else {
                sessions = client.getSessions(project.getId(), workflowName, Optional.fromNullable(lastId));
            }
        }

        EntityCollectionPrinter<RestSession> printer = new EntityCollectionPrinter<>();
        printer.field("SESSION ID", s -> Long.toString(s.getId()));
        printer.field("PROJECT", s -> s.getProject().getName());
        printer.field("WORKFLOW", s -> s.getWorkflow().getName());
        printer.field("SESSION TIME", s -> TimeUtil.formatTime(s.getSessionTime()));
        printer.field("LAST ATTEMPTED", s -> s.getLastAttempt().transform(a -> TimeUtil.formatTime(a.getCreatedAt())).or(""));
        printer.field("KILLED", s -> s.getLastAttempt().transform(a -> a.getCancelRequested()).or(false).toString());
        printer.field("STATUS", s -> status(s).toUpperCase());

        printer.print(format, Lists.reverse(sessions), out);

        out.println();
        out.flush();

        if (sessions.isEmpty()) {
            err.println("Use `digdag start` to start a session.");
        }
    }

    private void printSession(RestSession session)
            throws IOException
    {
        EntityPrinter<RestSession> printer = new EntityPrinter<>();

        printer.field("session id", s -> Long.toString(s.getId()));
        printer.field("attempt id", s -> s.getLastAttempt().transform(a -> Long.toString(a.getId())).or(""));
        printer.field("uuid", s -> s.getSessionUuid().toString());
        printer.field("project", s -> s.getProject().getName());
        printer.field("workflow", s -> s.getWorkflow().getName());
        printer.field("session time", s -> TimeUtil.formatTime(s.getSessionTime()));
        printer.field("retry attempt name", s -> s.getLastAttempt().transform(a -> a.getRetryAttemptName().or("")).or(""));
        printer.field("params", s -> s.getLastAttempt().transform(a -> a.getParams().toString()).or(""));
        printer.field("created at", s -> s.getLastAttempt().transform(a -> TimeUtil.formatTime(a.getCreatedAt())).or(""));
        printer.field("kill requested", s -> Boolean.toString(s.getLastAttempt().transform(a -> a.getCancelRequested()).or(false)));
        printer.field("status", this::status);

        printer.print(format, session, out);
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

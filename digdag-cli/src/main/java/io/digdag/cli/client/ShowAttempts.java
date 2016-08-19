package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.digdag.cli.EntityCollectionPrinter;
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
                }
                catch (NumberFormatException ignore) {
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

        EntityCollectionPrinter<RestSessionAttempt> printer = new EntityCollectionPrinter<>();

        printer.field("SESSION ID", a -> Long.toString(a.getId()));
        printer.field("ATTEMPT ID", a -> Integer.toString(a.getProject().getId()));
        printer.field("PROJECT", a -> a.getProject().getName());
        printer.field("WORKFLOW", a -> a.getWorkflow().getName());
        printer.field("SESSION TIME", a -> TimeUtil.formatTime(a.getSessionTime()));
        printer.field("CREATED", a -> TimeUtil.formatTime(a.getCreatedAt()));
        printer.field("KILLED", a -> Boolean.toString(a.getCancelRequested()));
        printer.field("STATUS", a -> status(a).toUpperCase());
        printer.field("RETRY NAME", a -> a.getRetryAttemptName().or(""));

        if (sessionId == null) {
            attempts = client.getSessionAttempts(Optional.fromNullable(lastId));
        }
        else {
            attempts = client.getSessionAttempts(sessionId, Optional.fromNullable(lastId));
        }

        printer.print(format, Lists.reverse(attempts), out);

        out.println();
        out.flush();

        if (attempts.isEmpty()) {
            err.println("Use `digdag start` to start a session.");
        }
    }

    private String status(RestSessionAttempt attempt)
    {
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
        return status;
    }
}

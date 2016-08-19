package io.digdag.cli.client;

import io.digdag.cli.EntityPrinter;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.core.Version;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowAttempt
    extends ClientCommand
{
    public ShowAttempt(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
    }

    @Override
    public void mainWithClientException()
            throws Exception
    {
        switch (args.size()) {
            case 1:
                long attemptId = parseLongOrUsage(args.get(0));
                showSessionAttempt(attemptId);
                break;
            default:
                throw usage(null);
        }
    }

    private void showSessionAttempt(long attemptId) throws Exception {
        DigdagClient client = buildClient();

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        if (attempt == null) {
            throw systemExit("Attempt with id " + attemptId + " not found.");
        }

        printAttempt(attempt);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag attempt  <attempt-id>            show a single attempt");
        showCommonOptions();
        return systemExit(error);
    }

    private void printAttempt(RestSessionAttempt attempt)
            throws IOException
    {
        EntityPrinter<RestSessionAttempt> printer = new EntityPrinter<>();

        printer.field("session id", a -> Long.toString(a.getSessionId()));
        printer.field("attempt id", a -> Long.toString(a.getId()));
        printer.field("uuid", a -> a.getSessionUuid().toString());
        printer.field("project", a -> a.getProject().getName());
        printer.field("workflow", a -> a.getWorkflow().getName());
        printer.field("session time", a -> TimeUtil.formatTime(a.getSessionTime()));
        printer.field("retry attempt name", a -> a.getRetryAttemptName().or(""));
        printer.field("created at", a -> TimeUtil.formatTime(a.getCreatedAt()));
        printer.field("kill requested", a -> Boolean.toString(a.getCancelRequested()));
        printer.field("status", this::attemptStatus);

        printer.print(format, attempt, out);
    }

    private String attemptStatus(RestSessionAttempt attempt)
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

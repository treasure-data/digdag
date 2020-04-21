package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowAttempt
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
            throws Exception
    {
        switch (args.size()) {
            case 1:
                Id attemptId = parseAttemptIdOrUsage(args.get(0));
                showSessionAttempt(attemptId);
                break;
            default:
                throw usage(null);
        }
    }

    private void showSessionAttempt(Id attemptId) throws Exception {
        DigdagClient client = buildClient();

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        if (attempt == null) {
            throw systemExit("Attempt with id " + attemptId + " not found.");
        }

        printAttempt(attempt);
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " attempt  <attempt-id>            show a single attempt");
        showCommonOptions();
        return systemExit(error);
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

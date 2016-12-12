package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestTask;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowTask
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        showTasks(parseAttemptIdOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " tasks <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void showTasks(Id attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();

        int count = 0;
        for (RestTask task : client.getTasks(attemptId).getTasks()) {
            ln("   id: %s", task.getId());
            ln("   name: %s", task.getFullName());
            ln("   state: %s", task.getState());
            ln("   started: %s", task.getStartedAt().transform(TimeUtil::formatTime).or(""));
            ln("   updated: %s", TimeUtil.formatTime(task.getUpdatedAt()));
            ln("   config: %s", task.getConfig());
            ln("   parent: %s", task.getParentId().orNull());
            ln("   upstreams: %s", task.getUpstreams());
            ln("   export params: %s", task.getExportParams());
            ln("   store params: %s", task.getStoreParams());
            ln("   state params: %s", task.getStateParams());
            ln("");
            count++;
        }

        if (count == 0) {
            client.getSessionAttempt(attemptId);  // throws exception if attempt doesn't exist
        }
        ln("%d entries.", count);
    }
}

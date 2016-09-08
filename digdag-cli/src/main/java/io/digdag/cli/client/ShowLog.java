package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import io.digdag.cli.CommandContext;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import io.digdag.core.log.LogLevel;

import java.io.IOException;
import java.util.List;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowLog
    extends ClientCommand
{
    @Parameter(names = {"-v", "--verbose"})
    protected boolean verbose = false;

    @Parameter(names = {"-f", "--follow"})
    protected boolean follow = false;

    public ShowLog(CommandContext context)
    {
        super(context);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 1:
            showLogs(parseLongOrUsage(args.get(0)), Optional.absent());
            break;
        case 2:
            showLogs(parseLongOrUsage(args.get(0)), Optional.of(args.get(1)));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        ctx.err().println("Usage: " + ctx.programName() + " log <attempt-id> [+task name prefix]");
        ctx.err().println("    -v, --verbose                    show debug logs");
        ctx.err().println("    -f, --follow                     show new logs until attempt or task finishes");
        ctx.err().println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void showLogs(long attemptId, Optional<String> taskName)
        throws Exception
    {
        DigdagClient client = buildClient();

        LogLevel level = verbose ? null : LogLevel.INFO;
        TaskLogWatcher watcher = new TaskLogWatcher(client, attemptId, level, ctx.out());

        update(client, watcher, attemptId, taskName);

        int interval = 500;
        if (follow) {
            while (true) {
                if (isFinished(client, attemptId, taskName)) {
                    break;
                }
                else {
                    Thread.sleep(interval);
                    interval = Math.min(interval * 2, 10000);
                    boolean updated = update(client, watcher, attemptId, taskName);
                    if (updated) {
                        interval = 500;
                    }
                }
            }
        }
    }

    private boolean update(DigdagClient client, TaskLogWatcher watcher,
            long attemptId, Optional<String> taskName)
        throws IOException
    {
        List<RestLogFileHandle> handles;
        if (taskName.isPresent()) {
            handles = client.getLogFileHandlesOfTask(attemptId, taskName.get());
        }
        else {
            handles = client.getLogFileHandlesOfAttempt(attemptId);
        }

        return watcher.update(handles);
    }

    private boolean isFinished(DigdagClient client, long attemptId, Optional<String> taskName)
    {
        if (taskName.isPresent()) {
            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                return true;
            }
            for (RestTask task : client.getTasks(attemptId)) {
                if (task.getFullName().startsWith(taskName.get())) {
                    switch (task.getState()) {
                    case "blocked":
                    case "ready":
                    case "retry_waiting":
                    case "group_retry_waiting":
                    case "running":
                        return false;
                    case "planned":
                    case "group_error":
                    case "success":
                    case "error":
                    case "canceled":
                    default:
                    }
                }
            }
            return true;
        }
        else {
            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            return attempt.getDone();
        }
    }
}

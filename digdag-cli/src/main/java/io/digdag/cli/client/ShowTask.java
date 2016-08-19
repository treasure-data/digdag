package io.digdag.cli.client;

import com.google.common.base.Joiner;
import io.digdag.cli.EntityCollectionPrinter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestTask;
import io.digdag.core.Version;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowTask
    extends ClientCommand
{
    public ShowTask(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        showTasks(parseLongOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag tasks <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void showTasks(long attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();


        List<RestTask> tasks = client.getTasks(attemptId);

        if (tasks.isEmpty()) {
            client.getSessionAttempt(attemptId);  // throws exception if attempt doesn't exist
        }

        EntityCollectionPrinter<RestTask> printer = new EntityCollectionPrinter<>();

        printer.field("ID", task -> Long.toString(task.getId()));
        printer.field("NAME", RestTask::getFullName);
        printer.field("STATE", RestTask::getState);
        printer.field("PARENT", task -> task.getParentId().transform(String::valueOf).or(""));
        printer.field("UPSTREAMS", task -> Joiner.on(", ").join(task.getUpstreams()));

        printer.print(format, tasks, out);
    }
}

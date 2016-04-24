package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.Map;
import java.util.HashMap;

import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestSessionAttemptRequest;

import static java.util.Locale.ENGLISH;
import static io.digdag.cli.SystemExitException.systemExit;

public class Retry
    extends ClientCommand
{
    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"--keep-revision"})
    boolean keepRevision = false;

    @Parameter(names = {"--latest-revision"})
    boolean latestRevision = false;

    @Parameter(names = {"--revision"})
    String revision = null;

    @Parameter(names = {"--all"})
    boolean all = false;

    @Parameter(names = {"--resume"})
    boolean resume = false;

    @Parameter(names = {"--from"})
    String from = null;

    @Parameter(names = {"--name"})
    String retryAttemptName = null;

    public Retry(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        String error = "";
        if (!keepRevision && !latestRevision && revision == null) {
            error += "--keep-revision, --latest-revision, or --revision <name> option is required. ";
        }
        if (!all && !resume && from == null) {
            error += "--all, --resume, or --from <name> option is required. ";
        }
        if (retryAttemptName == null) {
            error += "--name <name> option is required.";
        }
        if (!error.isEmpty()) {
            throw usage(error);
        }

        if (keepRevision && latestRevision || latestRevision && revision != null || keepRevision && revision != null) {
            throw usage("Setting --keep-revision, --latest-revision, or --revision together is invalid.");
        }

        if (all && resume || resume && from != null || all && from != null) {
            throw usage("Setting --all, --resume, or --from together is invalid.");
        }

        if (resume || from != null) {
            throw new UnsupportedOperationException("Sorry, --resume and --from are not implemented yet");
        }

        retry(parseLongOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag restart <attempt-id>");
        err.println("  Options:");
        err.println("        --name <name>                unique identifier of this retry attempt");
        err.println("        --latest-revision            use the latest revision");
        err.println("        --keep-revision              keep the same revision");
        err.println("        --revision <name>            use a specific revision");
        err.println("        --all                        retry all tasks");
        err.println("        --resume                     retry failed tasks, canceled tasks and _error tasks (not implemented yet)");
        err.println("        --from <+name>               retry tasks after a specific task (not implemented yet)");
        err.println("");
        return systemExit(error);
    }

    public void retry(long attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);

        long workflowId;
        if (keepRevision) {
            // use the same workflow id
            Optional<Long> id = attempt.getWorkflow().getId();
            if (!id.isPresent()) {
                throw systemExit("Session attempt " + attemptId + " is non-stored workflow. Retrying a non-stored workflow is not supported.");
            }
            workflowId = id.get();
        }
        else if (latestRevision) {
            // get the latest workflow with the same name in the same project
            RestWorkflowDefinition def = client.getWorkflowDefinition(
                    attempt.getProject().getId(), attempt.getWorkflow().getName());
            workflowId = def.getId();
        }
        else {
            // get workflow in a specific revision with the same name in the same project
            RestWorkflowDefinition def = client.getWorkflowDefinition(
                    attempt.getProject().getId(), attempt.getWorkflow().getName(), revision);
            workflowId = def.getId();
        }

        RestSessionAttemptRequest request = RestSessionAttemptRequest.builder()
            .workflowId(workflowId)
            .sessionTime(attempt.getSessionTime().toInstant())
            .retryAttemptName(Optional.of(retryAttemptName))
            .params(attempt.getParams())
            .build();

        RestSessionAttempt newAttempt = client.startSessionAttempt(request);

        ln("Started a session attempt:");
        ln("  id: %d", newAttempt.getId());
        ln("  uuid: %s", newAttempt.getSessionUuid());
        ln("  project: %s", newAttempt.getProject().getName());
        ln("  workflow: %s", newAttempt.getWorkflow().getName());
        ln("  session time: %s", formatTime(newAttempt.getSessionTime()));
        ln("  retry attempt name: %s", newAttempt.getRetryAttemptName().or(""));
        ln("  params: %s", newAttempt.getParams());
        ln("  created at: %s", formatTime(newAttempt.getCreatedAt()));
        ln("");

        err.println("* Use `digdag attempts` to list attempts.");
        err.println(String.format(ENGLISH,
                    "* Use `digdag task %d` and `digdag log %d` to show status.",
                    newAttempt.getId(), newAttempt.getId()));
    }
}

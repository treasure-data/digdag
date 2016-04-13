package io.digdag.cli.client;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.time.Instant;
import java.time.LocalDateTime;
import java.io.File;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.SessionTimeTruncate;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;
import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Main.systemExit;

public class Retry
    extends ClientCommand
{
    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"--keep-revision"})
    boolean keepRevision = false;

    @Parameter(names = {"--last-revision"})
    boolean lastRevision = false;

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

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        String error = "";
        if (!keepRevision && !lastRevision && revision == null) {
            error += "--keep-revision, --last-revision, or --revision <name> option is required. ";
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

        if (keepRevision && lastRevision || lastRevision && revision != null || keepRevision && revision != null) {
            throw usage("Setting --keep-revision, --last-revision, or --revision together is invalid.");
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
        System.err.println("Usage: digdag restart <attempt-id>");
        System.err.println("  Options:");
        System.err.println("        --name <name>                unique identifier of this retry attempt");
        System.err.println("        --last-revision              use the last revision");
        System.err.println("        --keep-revision              keep the same revision");
        System.err.println("        --revision <name>            use a specific revision");
        System.err.println("        --all                        retry all tasks");
        System.err.println("        --resume                     retry failed tasks, canceled tasks and _error tasks (not implemented yet)");
        System.err.println("        --from <+name>               retry tasks after a specific task (not implemented yet)");
        System.err.println("");
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
        else if (lastRevision) {
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

        System.err.println("* Use `digdag attempts` to list attempts.");
        System.err.println(String.format(ENGLISH,
                    "* Use `digdag task %d` and `digdag log %d` to show status.",
                    newAttempt.getId(), newAttempt.getId()));
    }
}

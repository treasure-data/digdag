package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import io.digdag.cli.ParameterValidator;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestWorkflowDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.digdag.cli.SystemExitException.systemExit;
import static java.util.Locale.ENGLISH;

public class Retry
    extends ClientCommand
{
    @Parameter(names = {"-p", "--param"}, validateWith = ParameterValidator.class)
    List<String> paramsList = new ArrayList<>();
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

    @Parameter(names = {"--resume-from"})
    String resumeFrom = null;

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
        if (!keepRevision && !latestRevision && revision == null) {
            error += "--keep-revision, --latest-revision, or --revision <name> option is required. ";
        }
        if (!all && !resume && resumeFrom == null) {
            error += "--all, --resume, or --resume-from <+name> option is required. ";
        }
        if (!error.isEmpty()) {
            throw usage(error);
        }

        if (keepRevision && latestRevision || latestRevision && revision != null || keepRevision && revision != null) {
            throw usage("Setting --keep-revision, --latest-revision, or --revision together is invalid.");
        }

        if (all && resume || resume && resumeFrom != null || all && resumeFrom != null) {
            throw usage("Setting --all, --resume, or --resume-from together is invalid.");
        }

        retry(parseAttemptIdOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " retry <attempt-id>");
        err.println("  Options:");
        err.println("        --name <name>                unique identifier of this retry attempt instead of auto-generated UUID");
        err.println("        --latest-revision            use the latest revision");
        err.println("        --keep-revision              keep the same revision");
        err.println("        --revision <name>            use a specific revision");
        err.println("        --all                        retry all tasks");
        err.println("        --resume                     retry only non-successful groups and tasks");
        err.println("        --resume-from <+name>        retry from a specific task");
        err.println("");
        return systemExit(error);
    }

    private void retry(Id attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);

        Id workflowId;
        if (keepRevision) {
            // use the same workflow id
            Optional<Id> id = attempt.getWorkflow().getId();
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

        if (retryAttemptName == null) {
            retryAttemptName = UUID.randomUUID().toString();
        }

        RestSessionAttemptRequest request = RestSessionAttemptRequest.builder()
            .workflowId(workflowId)
            .sessionTime(attempt.getSessionTime().toInstant())
            .retryAttemptName(Optional.of(retryAttemptName))
            .params(attempt.getParams())
            .build();

        if (resumeFrom != null) {
            request = RestSessionAttemptRequest.copyWithResume(
                    request,
                    RestSessionAttemptRequest.ResumeFrom.of(attemptId, resumeFrom));
        }
        else if (resume) {
            request = RestSessionAttemptRequest.copyWithResume(
                    request,
                    RestSessionAttemptRequest.ResumeFailed.of(attemptId));
        }

        RestSessionAttempt newAttempt = client.startSessionAttempt(request);

        ln("Started a session attempt:");
        ln("  session id: %s", newAttempt.getSessionId());
        ln("  attempt id: %s", newAttempt.getId());
        ln("  uuid: %s", newAttempt.getSessionUuid());
        ln("  project: %s", newAttempt.getProject().getName());
        ln("  workflow: %s", newAttempt.getWorkflow().getName());
        ln("  session time: %s", TimeUtil.formatTime(newAttempt.getSessionTime()));
        ln("  retry attempt name: %s", newAttempt.getRetryAttemptName().or(""));
        ln("  params: %s", newAttempt.getParams());
        ln("  created at: %s", TimeUtil.formatTime(newAttempt.getCreatedAt()));
        ln("");

        err.printf("* Use `" + programName + " session %s` to show session status.%n", newAttempt.getSessionId());
        err.println(String.format(ENGLISH,
                "* Use `" + programName + " task %s` and `" + programName + " log %s` to show task status and logs.",
                newAttempt.getId(), newAttempt.getId()));
    }
}

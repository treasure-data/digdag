package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Optional;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;

import java.util.List;
import java.util.UUID;

import static io.digdag.cli.SystemExitException.systemExit;

public class Backfill
    extends ClientCommand
{
    @Parameter(names = {"-f", "--from"})
    String fromTimeString;

    @Parameter(names = {"--name"})
    String retryAttemptName;

    @Parameter(names = {"--count"})
    Integer count;

    // TODO -n for count
    // TODO -t for to-time

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 2) {
            throw usage(null);
        }

        if (fromTimeString == null) {
            throw new ParameterException("--from option is required");
        }

        backfill(args.get(0), args.get(1));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " backfill <project-name> <workflow-name>");
        err.println("  Options:");
        err.println("    -f, --from 'yyyy-MM-dd[ HH:mm:ss]'  timestamp to start backfill from (required)");
        err.println("        --name NAME                  retry attempt name");
        err.println("    -d, --dry-run                    tries to backfill and validates the results but does nothing");
        err.println("        --count N                    number of sessions to run from the time (default: all sessions until the next schedule time)");
        showCommonOptions();
        return systemExit(error);
    }

    private void backfill(String projName, String workflowName)
        throws Exception
    {
        LocalTimeOrInstant fromTime = LocalTimeOrInstant.of(
                    TimeUtil.parseLocalTime(fromTimeString,
                        "--from must be hourly, daily, now, \"yyyy-MM-dd\", or \"yyyy-MM-dd HH:mm:SS\" format"));

        DigdagClient client = buildClient();

        RestSchedule sched = findScheduleByWorkflowName(client, projName, workflowName);

        if (sched == null) {
            // confirm that project and workflow exist, otherwise throws an exception
            RestProject proj = client.getProject(projName);
            RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), workflowName);
            throw systemExit("Schedule is not set to the workflow");
        }

        RestWorkflowSessionTime truncatedTime = client.getWorkflowTruncatedSessionTime(sched.getWorkflow().getId(), fromTime);

        if (retryAttemptName == null) {
            retryAttemptName = UUID.randomUUID().toString();
        }

        List<RestSessionAttempt> attempts = client.backfillSchedule(
                sched.getId(),
                truncatedTime.getSessionTime().toInstant(),
                retryAttemptName,
                Optional.fromNullable(count),
                dryRun);

        ln("Session attempts:");
        for (RestSessionAttempt attempt : attempts) {
            ln("  id: %d", attempt.getId());
            ln("  uuid: %s", attempt.getSessionUuid());
            ln("  project: %s", attempt.getProject().getName());
            ln("  workflow: %s", attempt.getWorkflow().getName());
            ln("  session time: %s", TimeUtil.formatTime(attempt.getSessionTime()));
            ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
            ln("  params: %s", attempt.getParams());
            ln("  created at: %s", TimeUtil.formatTime(attempt.getCreatedAt()));
            ln("");
        }

        if (dryRun || attempts.isEmpty()) {
            err.println("No session attempts started.");
        }
        else {
            err.println("Backfill session attempts started.");
            err.println("Use `" + programName + " sessions` to show the session attempts.");
        }
    }

    private static RestSchedule findScheduleByWorkflowName(DigdagClient client,
            String projName, String workflowName)
    {
        for (RestSchedule sched : client.getSchedules()) {
            if (projName.equals(sched.getProject().getName()) &&
                    workflowName.equals(sched.getWorkflow().getName())) {
                return sched;
            }
        }
        return null;
    }
}

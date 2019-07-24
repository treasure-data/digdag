package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.ParameterValidator;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.SessionTimeTruncate;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.spi.metrics.DigdagMetrics;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static java.util.Locale.ENGLISH;

public class Start
    extends ClientCommand
{
    @Parameter(names = {"-p", "--param"}, validateWith = ParameterValidator.class)
    List<String> paramsList = new ArrayList<>();
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"--retry"})
    String retryAttemptName = null;

    @Parameter(names = {"--session"})
    String sessionString = null;

    @Parameter(names = {"--revision"})
    String revision = null;

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 2) {
            throw usage(null);
        }
        if (sessionString == null) {
            throw usage("--session option is required");
        }
        start(args.get(0), args.get(1));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " start <project-name> <name>");
        err.println("  Options:");
        err.println("        --session <hourly | daily | now | yyyy-MM-dd | \"yyyy-MM-dd HH:mm:ss\">  set session_time to this time (required)");
        err.println("        --revision <name>            use a past revision");
        err.println("        --retry NAME                 set retry attempt name to a new session");
        err.println("    -d, --dry-run                    tries to start a session attempt but does nothing");
        err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        showCommonOptions();
        err.println("");
        err.println("  Examples:");
        err.println("    $ " + programName + " start myproj workflow1 --session 2016-01-01  # use this day as session_time");
        err.println("    $ " + programName + " start myproj workflow1 --session hourly      # use current hour's 00:00");
        err.println("    $ " + programName + " start myproj workflow1 --session daily       # use current day's 00:00:00");
        err.println("");
        return systemExit(error);
    }

    private void start(String projName, String workflowName)
        throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .addModules(binder -> {
                binder.bind(ConfigLoaderManager.class).in(Scopes.SINGLETON);
                binder.bind(DigdagMetrics.class).toInstance(StdDigdagMetrics.empty());
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);

        params = ParameterValidator.toMap(paramsList);
        Config overrideParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

        LocalTimeOrInstant time;
        SessionTimeTruncate mode;

        switch (sessionString) {
        case "hourly":
            time = LocalTimeOrInstant.of(Instant.now());
            mode = SessionTimeTruncate.HOUR;
            break;

        case "daily":
            time = LocalTimeOrInstant.of(Instant.now());
            mode = SessionTimeTruncate.DAY;
            break;

        case "now":
            time = LocalTimeOrInstant.of(Instant.now());
            mode = null;
            break;

        default:
            time = LocalTimeOrInstant.of(
                    TimeUtil.parseLocalTime(sessionString,
                        "--session must be hourly, daily, now, \"yyyy-MM-dd\", or \"yyyy-MM-dd HH:mm:SS\" format"));
            mode = null;
        }

        DigdagClient client = buildClient();

        RestProject proj = client.getProject(projName);

        RestWorkflowDefinition def;
        if (revision == null) {
            def = client.getWorkflowDefinition(proj.getId(), workflowName);
        }
        else {
            def = client.getWorkflowDefinition(proj.getId(), workflowName, revision);
        }

        RestWorkflowSessionTime truncatedTime = client.getWorkflowTruncatedSessionTime(def.getId(), time, mode);

        RestSessionAttemptRequest request = RestSessionAttemptRequest.builder()
            .workflowId(def.getId())
            .sessionTime(truncatedTime.getSessionTime().toInstant())
            .retryAttemptName(Optional.fromNullable(retryAttemptName))
            .params(overrideParams)
            .build();

        if (dryRun) {
            ln("Session attempt:");
            ln("  session id: (dry run)");
            ln("  attempt id: (dry run)");
            ln("  uuid: (dry run)");
            ln("  project: %s", def.getProject().getName());
            ln("  workflow: %s", def.getName());
            ln("  session time: %s", TimeUtil.formatTime(request.getSessionTime()));
            ln("  retry attempt name: %s", request.getRetryAttemptName().or(""));
            ln("  params: %s", request.getParams());
            //ln("  created at: (dry run)");
            ln("");

            err.println("Session attempt is not started.");
        }
        else {
            RestSessionAttempt newAttempt;
            try {
                newAttempt = client.startSessionAttempt(request);
            }
            catch (ClientErrorException ex) {
                if (ex.getResponse().getStatusInfo().equals(Response.Status.CONFLICT)) {
                    // 409 Conflict response contains RestSessionAttempt in its body
                    RestSessionAttempt conflictedAttempt;
                    try {
                        conflictedAttempt = ex.getResponse().readEntity(RestSessionAttempt.class);
                    }
                    catch (Exception readEntityError) {
                        throw systemExit(String.format(ENGLISH,
                                    "A session for the requested session_time already exists (session_time=%s)" +
                                    "\nhint: use `" + programName + " retry <attempt-id> --latest-revision --all` command to run the session again for the same session_time",
                                    truncatedTime.getSessionTime()));
                    }
                    throw systemExit(String.format(ENGLISH,
                                "A session for the requested session_time already exists (session_id=%s, attempt_id=%s, session_time=%s)" +
                                "\nhint: use `" + programName + " retry %s --latest-revision --all` command to run the session again for the same session_time",
                                conflictedAttempt.getSessionId(),
                                conflictedAttempt.getId(),
                                truncatedTime.getSessionTime(),
                                conflictedAttempt.getId()));
                }
                else {
                    throw ex;
                }
            }

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
                    "* Use `" + this.programName + " task %s` and `" + programName + " log %s` to show task status and logs.",
                        newAttempt.getId(), newAttempt.getId()));
        }
    }
}

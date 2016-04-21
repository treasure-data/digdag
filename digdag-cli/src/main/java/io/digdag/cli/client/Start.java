package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.Map;
import java.util.HashMap;
import java.time.Instant;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.SessionTimeTruncate;

import static java.util.Locale.ENGLISH;
import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;

public class Start
    extends ClientCommand
{
    @DynamicParameter(names = {"-p", "--param"})
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

    public Start(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

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
        err.println("Usage: digdag start <project-name> <name>");
        err.println("  Options:");
        err.println("        --session <hourly | daily | now | \"yyyy-MM-dd[ HH:mm:ss]\">  set session_time to this time (required)");
        err.println("        --revision <name>            use a past revision");
        err.println("        --retry NAME                 set retry attempt name to a new session");
        err.println("    -d, --dry-run                    tries to start a session attempt but does nothing");
        err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        showCommonOptions();
        err.println("");
        err.println("  Examples:");
        err.println("    $ digdag start myproj workflow1 --session \"2016-01-01 -07:00\"");
        err.println("    $ digdag start myproj workflow1 --session hourly      # use current hour's 00:00");
        err.println("    $ digdag start myproj workflow1 --session daily       # use current day's 00:00:00");
        err.println("");
        return systemExit(error);
    }

    public void start(String projName, String workflowName)
        throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .addModules(binder -> {
                binder.bind(ConfigLoaderManager.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);

        Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

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
                    parseLocalTime(sessionString,
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
            .params(overwriteParams)
            .build();

        if (dryRun) {
            ln("Session attempt:");
            ln("  id: (dry run)");
            ln("  uuid: (dry run)");
            ln("  project: %s", def.getProject().getName());
            ln("  workflow: %s", def.getName());
            ln("  session time: %s", formatTime(request.getSessionTime()));
            ln("  retry attempt name: %s", request.getRetryAttemptName().or(""));
            ln("  params: %s", request.getParams());
            //ln("  created at: (dry run)");
            ln("");

            err.println("Session attempt is not started.");
        }
        else {
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

            err.println("* Use `digdag sessions` to list session attempts.");
            err.println(String.format(ENGLISH,
                        "* Use `digdag task %d` and `digdag log %d` to show status.",
                        newAttempt.getId(), newAttempt.getId()));
        }
    }
}

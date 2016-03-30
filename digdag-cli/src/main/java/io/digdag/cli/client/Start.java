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
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestSessionAttemptPrepareRequest;
import io.digdag.client.api.RestSessionAttemptPrepareResult;
import io.digdag.client.api.ImmutableRestSessionAttemptPrepareRequest;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.SessionTimeTruncate;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;
import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Main.systemExit;

public class Start
    extends ClientCommand
{
    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-R", "--retry"})
    String retryAttemptName = null;

    @Parameter(names = {"--session"})
    String sessionString = null;

    @Parameter(names = {"--revision"})
    String revision = null;

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
        System.err.println("Usage: digdag start <repo-name> <+name>");
        System.err.println("  Options:");
        System.err.println("        --session <hourly | daily | now | \"yyyy-MM-dd[ HH:mm:ss] Z\">  set session_time to this time (required)");
        System.err.println("        --revision <name>            use a past revision");
        System.err.println("    -R, --retry NAME                 set attempt name to retry a session");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        ClientCommand.showCommonOptions();
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ digdag start my_repo +workflow1 --session \"2016-01-01 -07:00\"");
        System.err.println("    $ digdag start my_repo +workflow1 --session hourly      # use current hour's 00:00");
        System.err.println("    $ digdag start my_repo +workflow1 --session daily       # use current day's 00:00:00");
        System.err.println("");
        return systemExit(error);
    }

    public void start(String repoName, String workflowName)
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

        Config overwriteParams = loadParams(cf, loader, paramsFile, params);

        RestSessionAttemptPrepareRequest prepareRequest = buildPrepareRequest(repoName, workflowName);

        DigdagClient client = buildClient();

        RestSessionAttemptPrepareResult prepareResult = client.prepareSessionAttempt(prepareRequest);

        RestSessionAttemptRequest request = RestSessionAttemptRequest.builder()
            .workflowId(prepareResult.getWorkflowId())
            .sessionTime(prepareResult.getSessionTime().toInstant())
            .retryAttemptName(Optional.fromNullable(retryAttemptName))
            .params(overwriteParams)
            .build();

        RestSessionAttempt attempt = client.startSessionAttempt(request);

        ln("Started a session attempt:");
        ln("  id: %d", attempt.getId());
        ln("  uuid: %s", attempt.getSessionUuid());
        ln("  repository: %s", attempt.getRepository().getName());
        ln("  workflow: %s", attempt.getWorkflowName());
        ln("  session time: %s", formatTime(attempt.getSessionTime()));
        ln("  retry attempt name: %s", attempt.getRetryAttemptName().or(""));
        ln("  params: %s", attempt.getParams());
        ln("  created at: %s", formatTime(attempt.getCreatedAt()));
        ln("");

        System.err.println("Use `digdag sessions` to show status.");
    }

    private RestSessionAttemptPrepareRequest buildPrepareRequest(String repoName, String workflowName)
        throws SystemExitException
    {
        ImmutableRestSessionAttemptPrepareRequest.Builder builder =
            RestSessionAttemptPrepareRequest.builder()
            .repositoryName(repoName)
            .revision(Optional.fromNullable(revision))
            .workflowName(workflowName);

        switch (sessionString) {
        case "hourly":
            return builder
                .sessionTime(LocalTimeOrInstant.of(Instant.now()))
                .sessionTimeTruncate(Optional.of(SessionTimeTruncate.HOUR))
                .build();

        case "daily":
            return builder
                .sessionTime(LocalTimeOrInstant.of(Instant.now()))
                .sessionTimeTruncate(Optional.of(SessionTimeTruncate.DAY))
                .build();

        case "now":
            return builder
                .sessionTime(LocalTimeOrInstant.of(Instant.now()))
                .sessionTimeTruncate(Optional.absent())
                .build();

        default:
            LocalDateTime local = parseLocalTime(sessionString,
                    "--session must be hourly, daily, now, \"yyyy-MM-dd\", or \"yyyy-MM-dd HH:mm:SS\" format");
            return builder
                .sessionTime(LocalTimeOrInstant.of(local))
                .sessionTimeTruncate(Optional.absent())
                .build();
        }
    }
}

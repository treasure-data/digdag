package io.digdag.cli.client;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.time.Instant;
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
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionRequest;
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

    @Parameter(names = {"--now"})
    boolean now = false;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (now) {
            if (args.size() != 2) {
                throw usage(null);
            }
            start(args.get(0), args.get(1), ScheduleTime.alignedNow());
        }
        else {
            if (args.size() != 3) {
                throw usage(null);
            }
            start(args.get(0), args.get(1), parseTime(args.get(2)));
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag start <repo-name> <+name> [--now or \"yyyy-MM-dd HH:mm:ss Z\"]");
        System.err.println("  Options:");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        System.err.println("    -R, --retry NAME                 set attempt name to retry a session");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void start(String repoName, String workflowName, Instant sessiontime)
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

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.loadParameterizedFile(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        RestSessionRequest request = RestSessionRequest.builder()
            .repositoryName(repoName)
            .workflowName(workflowName)
            .sessionTime(sessiontime)
            .retryAttemptName(Optional.fromNullable(retryAttemptName))
            .params(overwriteParams)
            .build();

        DigdagClient client = buildClient();
        RestSession session = client.startSession(request);

        ln("Started a session:");
        ln("  id: %d", session.getId());
        ln("  uuid: %s", session.getSessionUuid());
        ln("  repository: %s", session.getRepository().getName());
        ln("  workflow: %s", session.getWorkflowName());
        ln("  session time: %s", formatTime(session.getSessionTime()));
        ln("  retry attempt name: %s", session.getRetryAttemptName().or(""));
        ln("  params: %s", session.getParams());
        ln("  created at: %s", formatTime(session.getId()));
        ln("");

        System.err.println("Use `digdag sessions` to show session status.");
    }
}

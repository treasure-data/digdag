package io.digdag.cli.client;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.io.File;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.cli.ArgumentConfigLoader;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSession;
import static io.digdag.cli.Main.systemExit;

public class Start
    extends ClientCommand
{
    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-u", "--unique"})
    String uniqueName = null;

    @Override
    public void main()
        throws Exception
    {
        if (args.size() != 2) {
            throw usage(null);
        }
        start(args.get(0), args.get(1));
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag start <repo-name> <workflow-name>");
        System.err.println("  Options:");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        System.err.println("    -u, --unique NAME                unique name of this session");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void start(String repoName, String workflowName)
        throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);

        Config sessionParams = cf.create();
        if (paramsFile != null) {
            sessionParams.setAll(loader.load(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            sessionParams.set(pair.getKey(), pair.getValue());
        }

        String sessionName = uniqueName;
        if (sessionName == null) {
            sessionName = UUID.randomUUID().toString();
        }

        DigdagClient client = buildClient();
        RestSession session = client.startSession(sessionName, repoName, workflowName, sessionParams);

        modelPrinter().print(session);
    }
}

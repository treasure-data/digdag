package io.digdag.cli.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdOut;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.Version;
import io.digdag.core.config.ConfigLoaderManager;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.client.ProjectUtil.showUploadedProject;

public class Push
    extends ClientCommand
{
    @Parameter(names = {"--project"})
    String projectDirName = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    public Push(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        if (revision == null) {
            throw usage("-r, --revision option is required");
        }
        push(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag push <project> -r <revision>");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -r, --revision REVISION          revision name");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        //err.println("        --time-revision              use current time as the revision name");
        showCommonOptions();
        return systemExit(error);
    }

    private void push(String projName)
        throws Exception
    {
        Path dir = Files.createDirectories(Paths.get(".digdag/tmp"));
        Path archivePath = Files.createTempFile(dir, "archive-", ".tar.gz");
        archivePath.toFile().deleteOnExit();

        Injector injector = new DigdagEmbed.Bootstrap()
                .withWorkflowExecutor(false)
                .withScheduleExecutor(false)
                .withLocalAgent(false)
                .addModules(binder -> {
                    binder.bind(Archiver.class).in(Scopes.SINGLETON);
                    binder.bind(PrintStream.class).annotatedWith(StdOut.class).toInstance(out);
                    binder.bind(PrintStream.class).annotatedWith(StdErr.class).toInstance(err);
                })
                .initialize()
                .getInjector();

        ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);

        // read parameters
        Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

        // load project
        Path projectPath = (projectDirName == null) ?
            Paths.get("").toAbsolutePath() :
            Paths.get(projectDirName).normalize().toAbsolutePath();
        injector.getInstance(Archiver.class).createArchive(projectPath, archivePath, overwriteParams);

        DigdagClient client = buildClient();
        RestProject proj = client.putProjectRevision(projName, revision, archivePath.toFile());
        showUploadedProject(out, proj);
    }
}

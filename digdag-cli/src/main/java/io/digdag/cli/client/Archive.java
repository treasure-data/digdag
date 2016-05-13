package io.digdag.cli.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.Command;
import io.digdag.cli.Main;
import io.digdag.cli.Run;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdOut;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.YamlMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;

public class Archive
        extends Command
{
    @Parameter(names = {"-f", "--file"})
    String dagfilePath = Run.DEFAULT_DAGFILE;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-o", "--output"})
    String output = "digdag.archive.tar.gz";

    public Archive(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }
        archive();
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag archive [options...]");
        err.println("  Options:");
        err.println("    -f, --file PATH                  use this file to load a project (default: digdag.dig)");
        err.println("    -o, --output ARCHIVE.tar.gz      output path (default: digdag.archive.tar.gz)");
        Main.showCommonOptions(err);
        return systemExit(error);
    }

    private void archive()
            throws IOException
    {
        Injector injector = new DigdagEmbed.Bootstrap()
                .withWorkflowExecutor(false)
                .withScheduleExecutor(false)
                .withLocalAgent(false)
                .addModules(binder -> {
                    binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
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

        injector.getInstance(Archiver.class).createArchive(Paths.get(dagfilePath), Paths.get(output), overwriteParams);

        out.println("Created " + output + ".");
        out.println("Use `digdag upload <path.tar.gz> <project> <revision>` to upload it a server.");
        out.println("");
        out.println("  Examples:");
        out.println("    $ digdag upload " + output + " $(basename $(pwd)) $(date +%Y%m%d-%H%M%S)");
        out.println("    $ digdag upload " + output + " $(git rev-parse --abbrev-ref HEAD) $(git rev-parse HEAD)");
        out.println("");
    }
}

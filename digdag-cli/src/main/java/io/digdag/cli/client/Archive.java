package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.Command;
import io.digdag.cli.Main;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdOut;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.YamlMapper;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.cli.SystemExitException.systemExit;

public class Archive
        extends Command
{
    @Parameter(names = {"--project"})
    String projectDirName = null;

    @Parameter(names = {"-o", "--output"})
    String output = "digdag.archive.tar.gz";

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
        err.println("Usage: " + programName + " archive [options...]");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -o, --output ARCHIVE.tar.gz      output path (default: digdag.archive.tar.gz)");
        Main.showCommonOptions(env, err);
        return systemExit(error);
    }

    private void archive()
            throws Exception
    {
        ConfigElement systemConfig = ConfigElement.fromJson("{ \"database.migrate\" : false } }");

        try (DigdagEmbed digdag = new DigdagEmbed.Bootstrap()
                .setSystemConfig(systemConfig)
                .withWorkflowExecutor(false)
                .withScheduleExecutor(false)
                .withLocalAgent(false)
                .withTaskQueueServer(false)
                .addModules(binder -> {
                    binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
                    binder.bind(Archiver.class).in(Scopes.SINGLETON);
                    binder.bind(PrintStream.class).annotatedWith(StdOut.class).toInstance(out);
                    binder.bind(PrintStream.class).annotatedWith(StdErr.class).toInstance(err);
                })
                .initializeWithoutShutdownHook()) {
            archive(digdag.getInjector());
        }
    }

    private void archive(Injector injector)
            throws IOException
    {
        // load project
        Path projectPath = (projectDirName == null) ?
            Paths.get("").toAbsolutePath() :
            Paths.get(projectDirName).normalize().toAbsolutePath();
        injector.getInstance(Archiver.class).createArchive(projectPath, Paths.get(output));

        out.println("Created " + output + ".");
        out.println("Use `" + programName + " upload <path.tar.gz> <project> <revision>` to upload it a server.");
        out.println("");
        out.println("  Examples:");
        out.println("    $ " + programName + " upload " + output + " $(basename $(pwd)) -r $(date +%Y%m%d-%H%M%S)");
        out.println("    $ " + programName + " upload " + output + " $(git rev-parse --abbrev-ref HEAD) -r $(git rev-parse HEAD)");
        out.println("");
    }
}

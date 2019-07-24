package io.digdag.cli.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdOut;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.cli.YamlMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.spi.metrics.DigdagMetrics;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.client.ProjectUtil.showUploadedProject;

public class Push
    extends ClientCommand
{
    @Parameter(names = {"--project"})
    String projectDirName = null;

    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    @Parameter(names = {"--schedule-from"})
    String scheduleFromString = null;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        push(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " push <project> -r <revision>");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -r, --revision REVISION          specific revision name instead of auto-generated UUID");
        err.println("        --schedule-from \"yyyy-MM-dd HH:mm:ss Z\"  start schedules from this time instead of current time");
        showCommonOptions();
        return systemExit(error);
    }

    private void push(String projName)
        throws Exception
    {
        Preconditions.checkNotNull(projName, "projName");
        if (projName.isEmpty()) {
            throw usage("project name cannot be empty");
        }

        Path dir = Files.createDirectories(Paths.get(".digdag/tmp"));
        Path archivePath = Files.createTempFile(dir, "archive-", ".tar.gz");
        archivePath.toFile().deleteOnExit();

        ConfigElement systemConfig = ConfigElement.fromJson("{ \"database.migrate\" : false } }");

        Injector injector = new DigdagEmbed.Bootstrap()
                .setSystemConfig(systemConfig)
                .withWorkflowExecutor(false)
                .withScheduleExecutor(false)
                .withLocalAgent(false)
                .addModules(binder -> {
                    binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
                    binder.bind(Archiver.class).in(Scopes.SINGLETON);
                    binder.bind(PrintStream.class).annotatedWith(StdOut.class).toInstance(out);
                    binder.bind(PrintStream.class).annotatedWith(StdErr.class).toInstance(err);
                    binder.bind(DigdagMetrics.class).toInstance(StdDigdagMetrics.empty());
                })
                .initialize()
                .getInjector();

        // schedule_from will be server's current time if not set
        Optional<Instant> scheduleFrom;
        if (scheduleFromString == null) {
            scheduleFrom = Optional.absent();
        }
        else {
            scheduleFrom = Optional.of(TimeUtil.parseTime(scheduleFromString, "--schedule-from"));
        }

        // load project
        Path projectPath = (projectDirName == null) ?
            Paths.get("").toAbsolutePath() :
            Paths.get(projectDirName).normalize().toAbsolutePath();
        List<String> workflows = injector.getInstance(Archiver.class).createArchive(projectPath, archivePath);
        out.println("Workflows:");
        if (workflows.isEmpty()) {
            out.println("  WARNING: This project doesn't include workflows. Usually, this is a mistake.");
            out.println("           Please make sure that all *.dig files are on the top directory.");
            out.println("           *.dig files in subdirectories are not recognized as workflows.");
            out.println("");
        }
        else {
            for (String workflow : workflows) {
                out.println("  " + workflow);
            }
        }

        DigdagClient client = buildClient();
        if ("".equals(revision)) {
            throw usage("revision cannot be empty");
        }
        if (revision == null) {
            revision = Upload.generateDefaultRevisionName();
        }
        RestProject proj = client.putProjectRevision(projName, revision, archivePath.toFile(), scheduleFrom);
        showUploadedProject(out, proj, programName);
    }
}

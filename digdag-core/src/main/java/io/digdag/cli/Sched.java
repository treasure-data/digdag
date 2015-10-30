package io.digdag.cli;

import java.util.List;
import java.util.Date;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.util.TimeZone;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import io.digdag.DigdagEmbed;
import io.digdag.core.*;
import io.digdag.cli.Main.SystemExitException;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Sched
{
    private static Logger logger = LoggerFactory.getLogger(Sched.class);

    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("o", "output")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        File workflowPath = new File(argv.get(0));

        String workflowFileName = workflowPath.getName().replaceFirst("\\.\\w*$", "");
        File outputPath = Optional.fromNullable((String) op.valueOf("o")).transform(it -> new File(it)).or(
                    new File(workflowPath.getParent(), workflowFileName));

        new Sched(workflowPath, outputPath).sched();
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sched <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -o, --output DIR                  store execution results to this directory (default: same name with workflow file name)");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    private final File workflowPath;
    private final File outputPath;

    public Sched(File workflowPath, final File outputPath)
    {
        this.workflowPath = workflowPath;
        this.outputPath = outputPath;
    }

    public void sched() throws Exception
    {
        DigdagEmbed embed = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(HistoryFiles.class).toInstance(new HistoryFiles(outputPath));
            })
            .overrideModules(modules -> {
                Module mod = Modules.override(modules).with(binder -> {
                    binder.bind(ScheduleStarter.class).to(ScheduleStarterWithResumeState.class);
                });
                return asList(mod);
            })
            .initialize();
        try {
            sched(embed.getInjector());
        }
        finally {
            // close explicitly so that ResumeStateFileManager.preDestroy runs before closing h2 database
            embed.destroy();
        }
    }

    public void sched(Injector injector) throws Exception
    {
        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);
        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);

        outputPath.mkdirs();

        List<WorkflowSource> workflowSources = Run.loadWorkflowSources(loader.loadFile(workflowPath));

        localSite.scheduleWorkflows(workflowSources, new Date());
        // TODO set next schedule time from history

        localSite.startLocalAgent();
        localSite.startScheduler();

        localSite.run();
    }

    private static class ScheduleStarterWithResumeState
            extends ScheduleStarter
    {
        private final FileMapper mapper;
        private final HistoryFiles hist;
        private final ResumeStateFileManager resumeStateFiles;
        private final RepositoryStoreManager rm;

        @Inject
        public ScheduleStarterWithResumeState(
                ConfigSourceFactory cf,
                RepositoryStoreManager rm,
                SessionExecutor exec,
                ResumeStateFileManager resumeStateFiles,
                HistoryFiles hist,
                FileMapper mapper,
                SessionStoreManager sessionStoreManager)
        {
            super(cf, rm, exec);
            this.hist = hist;
            this.mapper = mapper;
            this.resumeStateFiles = resumeStateFiles;
            this.rm = rm;
        }

        @Override
        public StoredSession start(int workflowId, TimeZone timeZone, Date scheduleTime)
        {
            StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

            File dir = hist.getSessionDir(wf, timeZone, scheduleTime);
            dir.mkdirs();

            StoredSession session = super.start(workflowId, timeZone, scheduleTime);
            logger.debug("Submitting {}", session);

            mapper.writeFile(new File(dir, "workflow.yml"), ImmutableMap.of(wf.getName(), wf.getConfig()));

            mapper.writeFile(new File(dir, "params.yml"), session.getParams());

            resumeStateFiles.startUpdate(new File(dir, "state.yml"), session);

            return session;
        }
    }
}

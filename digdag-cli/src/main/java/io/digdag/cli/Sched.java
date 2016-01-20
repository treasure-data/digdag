package io.digdag.cli;

import java.util.List;
import java.util.Date;
import java.util.stream.Collectors;
import java.io.File;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.beust.jcommander.Parameter;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.schedule.ScheduleHandler;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Sched
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Sched.class);

    @Parameter(names = {"-o", "--output"})
    String output = null;

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }

        String workflowPath = args.get(0);

        if (output == null) {
            File workflowFile = new File(workflowPath);
            String workflowFileName = workflowFile.getName().replaceFirst("\\.\\w*$", "");
            output = new File(workflowFile.getParent(), workflowFileName).toString();
        }

        //sched(args.get(0));
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sched <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -o, --output DIR                 store execution results to this directory (default: same name with workflow file name)");
        Main.showCommonOptions();
        return systemExit(error);
    }

    // TODO use Dagfile
    /*
    private void sched(String workflowPath)
            throws Exception
    {
        File outputFile = new File(output);

        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(HistoryFiles.class).toInstance(new HistoryFiles(outputFile));
                binder.bind(ResumeStateFileManager.class).in(Scopes.SINGLETON);
                binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
            })
            .overrideModules(modules -> {
                Module mod = Modules.override(modules).with(binder -> {
                    binder.bind(ScheduleHandler.class).to(ScheduleHandlerWithResumeState.class);
                });
                return asList(mod);
            })
            .initialize()
            .getInjector();

        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);

        outputFile.mkdirs();

        List<WorkflowSource> workflowSources = loader.load(new File(workflowPath), cf.create()).convert(WorkflowSourceList.class).get();

        localSite.scheduleWorkflows(workflowSources, new Date());
        // TODO set next schedule time from history

        localSite.startLocalAgent();
        localSite.startScheduler();
        localSite.startMonitor();

        localSite.run();
    }

    private static class ScheduleHandlerWithResumeState
            extends ScheduleHandler
    {
        private final FileMapper mapper;
        private final HistoryFiles hist;
        private final ResumeStateFileManager resumeStateFiles;
        private final RepositoryStoreManager rm;

        @Inject
        public ScheduleHandlerWithResumeState(
                ConfigFactory cf,
                RepositoryStoreManager rm,
                WorkflowExecutor exec,
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
        public StoredSession start(int workflowId, Optional<String> from,
                TimeZone timeZone, ScheduleTime time)
                throws ResourceNotFoundException, ResourceConflictException
        {
            StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

            File dir = hist.getSessionDir(wf, timeZone, time.getScheduleTime());
            dir.mkdirs();

            StoredSession session = super.start(workflowId, from, timeZone, time);
            logger.debug("Submitting {}", session);

            mapper.writeFile(new File(dir, "workflow.yml"), ImmutableMap.of(wf.getName(), wf.getConfig()));

            mapper.writeFile(new File(dir, "params.yml"), session.getParams());

            // TODO generate command.sh file that includes
            //      digdag -p params.yml -w workflow.yml -f from

            resumeStateFiles.startUpdate(new File(dir, "state.yml"), session);

            return session;
        }
    }
    */
}

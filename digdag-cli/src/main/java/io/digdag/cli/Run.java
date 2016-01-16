package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.session.SessionOptions;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.yaml.YamlConfigLoader;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;

public class Run
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    @Parameter(names = {"-f", "--from"})
    String fromTaskName = null;

    @Parameter(names = {"-r", "--resume-state"})
    String resumeStateFilePath = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-s", "--show"})
    String visualizePath = null;

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        run(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag run <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -f, --from +NAME                 skip tasks before this task");
        System.err.println("    -r, --resume-state PATH.yml      path to resume state file");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        System.err.println("    -s, --show PATH.png              visualize result of execution and create a PNG file");
        // TODO add -p, --param K=V
        Main.showCommonOptions();
        return systemExit(error);
    }

    public void run(String workflowPath) throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(ResumeStateFileManager.class).in(Scopes.SINGLETON);
                binder.bind(FileMapper.class).in(Scopes.SINGLETON);
                binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final YamlConfigLoader rawLoader = injector.getInstance(YamlConfigLoader.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final ResumeStateFileManager rsm = injector.getInstance(ResumeStateFileManager.class);

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.load(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        Optional<ResumeState> resumeState = Optional.absent();
        if (resumeStateFilePath != null && loader.checkExists(new File(resumeStateFilePath))) {
            // jinja and !!include are disabled
            resumeState = Optional.of(rawLoader.loadFile(new File(resumeStateFilePath), Optional.absent(), Optional.absent()).convert(ResumeState.class));
        }

        WorkflowSource workflowSource = loadFirstWorkflowSource(loader.load(new File(workflowPath), overwriteParams));

        SessionOptions options = SessionOptions.builder()
            .skipTaskMap(resumeState.transform(it -> it.getReports()).or(ImmutableMap.of()))
            .build();

        List<StoredSession> sessions = localSite.startWorkflows(
                TimeZone.getDefault(),  // TODO configurable by cmdline argument
                WorkflowSourceList.of(ImmutableList.of(workflowSource)),
                Optional.fromNullable(fromTaskName),
                overwriteParams, new Date(), options);
        if (sessions.isEmpty()) {
            throw systemExit("No workflows to start");
        }
        StoredSession session = sessions.get(0);
        logger.debug("Submitting {}", session);

        localSite.startLocalAgent();
        localSite.startMonitor();

        // if resumeStateFilePath is not set, use workflow.yml.resume.yml
        File resumeResultPath = new File(Optional.fromNullable(resumeStateFilePath).or(workflowPath + ".resume.yml"));
        rsm.startUpdate(resumeResultPath, session);

        localSite.runUntilAny();

        ArrayList<StoredTask> failedTasks = new ArrayList<>();
        for (StoredTask task : localSite.getSessionStore().getTasks(session.getId(), 100, Optional.absent())) {  // TODO paging
            if (task.getState() == TaskStateCode.ERROR) {
                failedTasks.add(task);
            }
            logger.debug("  Task["+task.getId()+"]: "+task.getFullName());
            logger.debug("    parent: "+task.getParentId().transform(it -> Long.toString(it)).or("(root)"));
            logger.debug("    upstreams: "+task.getUpstreams().stream().map(it -> Long.toString(it)).collect(Collectors.joining(",")));
            logger.debug("    state: "+task.getState());
            logger.debug("    retryAt: "+task.getRetryAt());
            logger.debug("    config: "+task.getConfig());
            logger.debug("    taskType: "+task.getTaskType());
            logger.debug("    stateParams: "+task.getStateParams());
            logger.debug("    carryParams: "+task.getReport().transform(report -> report.getCarryParams()).or(cf.create()));
            logger.debug("    in: "+task.getReport().transform(report -> report.getInputs()).or(ImmutableList.of()));
            logger.debug("    out: "+task.getReport().transform(report -> report.getOutputs()).or(ImmutableList.of()));
            logger.debug("    error: "+task.getError());
        }

        if (visualizePath != null) {
            List<WorkflowVisualizerNode> nodes = localSite.getSessionStore().getTasks(session.getId(), 1024, Optional.absent())
                .stream()
                .map(it -> WorkflowVisualizerNode.of(it))
                .collect(Collectors.toList());
            Show.show(nodes, new File(visualizePath));
        }

        if (!failedTasks.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (StoredTask task : failedTasks) {
                sb.append(String.format("Task %s failed.%n", task.getFullName()));
            }
            sb.append(String.format("Use `digdag run %s -r %s` to restart this workflow.",
                        workflowPath, resumeResultPath));
            throw systemExit(sb.toString());
        }
        else if (resumeStateFilePath == null) {
            rsm.stopUpdate(resumeResultPath);
            rsm.shutdown();
            resumeResultPath.delete();
        }
    }

    // also used by Sched
    static WorkflowSource loadFirstWorkflowSource(final Config ast)
    {
        List<WorkflowSource> workflowSources = ast.convert(WorkflowSourceList.class).get();
        if (workflowSources.size() != 1) {
            if (workflowSources.isEmpty()) {
                throw new RuntimeException("Workflow file doesn't include definitions");
            }
            else {
                throw new RuntimeException("Workflow file includes more than one definitions");
            }
        }
        return workflowSources.get(0);
    }
}

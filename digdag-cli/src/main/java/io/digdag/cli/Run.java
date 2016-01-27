package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.time.Instant;
import java.time.ZoneId;
import java.util.stream.Collectors;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.agent.TaskRunnerManager;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.agent.SetThreadName;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.yaml.YamlConfigLoader;
import io.digdag.spi.TaskReport;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;

public class Run
    extends Command
{
    public static final String DEFAULT_DAGFILE = "digdag.yml";

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    @Parameter(names = {"-f", "--file"})
    String dagfilePath = null;

    @Parameter(names = {"-s", "--session"})
    String sessionStatePath = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    //@Parameter(names = {"-G", "--graph"})
    //String visualizePath = null;

    // TODO dry run

    private boolean runAsImplicit = false;

    // used by Main
    static Run asImplicit()
    {
        Run command = new Run();
        command.runAsImplicit = true;
        return command;
    }

    @Override
    public void main()
            throws Exception
    {
        if (runAsImplicit && args.isEmpty() && dagfilePath == null) {
            throw Main.usage(null);
        }

        if (dagfilePath == null) {
            dagfilePath = DEFAULT_DAGFILE;
        }

        String taskNamePattern;
        switch (args.size()) {
        case 0:
            taskNamePattern = null;
            break;
        case 1:
            taskNamePattern = args.get(0);
            if (!taskNamePattern.startsWith("+")) {
                throw usage("Task name must begin with '+': " + taskNamePattern);
            }
            break;
        default:
            throw usage(null);
        }
        run(taskNamePattern);
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag run [+task] [options...]");
        System.err.println("  Options:");
        System.err.println("    -f, --file PATH                  use this file to load tasks (default: digdag.yml)");
        System.err.println("    -s, --session PATH               use this directory to read and write session status");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        //System.err.println("    -g, --graph OUTPUT.png           visualize a task and exit");
        //System.err.println("    -d, --dry-run                    dry run mode");
        Main.showCommonOptions();
        return systemExit(error);
    }

    public void run(String taskNamePattern) throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(ResumeStateManager.class).in(Scopes.SINGLETON);
                binder.bind(FileMapper.class).in(Scopes.SINGLETON);
                binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
                binder.bind(Run.class).toInstance(this);  // used by TaskRunnerManagerWithSkip
            })
            .overrideModules((list) -> ImmutableList.of(Modules.override(list).with((binder) -> {
                binder.bind(TaskRunnerManager.class).to(TaskRunnerManagerWithSkip.class).in(Scopes.SINGLETON);
            })))
            .initialize()
            .getInjector();

        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final YamlConfigLoader rawLoader = injector.getInstance(YamlConfigLoader.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.load(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        if (sessionStatePath != null) {
            this.skipTaskReports = (fullName) -> rsm.readSuccessfulTaskReport(new File(sessionStatePath), fullName);
        }

        Dagfile dagfile = loader.load(new File(dagfilePath), overwriteParams).convert(Dagfile.class);
        if (taskNamePattern == null) {
            if (dagfile.getDefaultTaskName().isPresent()) {
                taskNamePattern = dagfile.getDefaultTaskName().get();
            }
            else {
                throw new ConfigException(String.format(
                            "default: option is not written at %s file. Please add default: option or add +NAME option to command line", dagfilePath));
            }
        }
        WorkflowDefinitionList defs = dagfile.getWorkflowList();

        StoredSessionAttemptWithSession attempt = localSite.storeAndStartWorkflows(
                ZoneId.systemDefault(),  // TODO configurable by cmdline argument
                defs,
                TaskMatchPattern.compile(taskNamePattern),
                dagfile.getDefaultParams(),
                overwriteParams);
        logger.debug("Submitting {}", attempt);

        localSite.startLocalAgent();
        localSite.startMonitor();

        // if sessionStatePath is not set, use workflow.yml.resume.yml
        File resumeResultPath = new File(Optional.fromNullable(sessionStatePath).or("digdag.status"));
        rsm.startUpdate(resumeResultPath, attempt);

        localSite.runUntilAny();

        ArrayList<StoredTask> failedTasks = new ArrayList<>();
        for (StoredTask task : localSite.getSessionStore().getTasksOfAttempt(attempt.getId())) {
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

        //if (visualizePath != null) {
        //    List<WorkflowVisualizerNode> nodes = localSite.getSessionStore().getTasks(attempt.getId(), 1024, Optional.absent())
        //        .stream()
        //        .map(it -> WorkflowVisualizerNode.of(it))
        //        .collect(Collectors.toList());
        //    Show.show(nodes, new File(visualizePath));
        //}

        if (!failedTasks.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%n"));
            sb.append(String.format("Task state is stored at %s directory.%n", resumeResultPath));
            for (StoredTask task : failedTasks) {
                sb.append(String.format("  Task %s failed.%n", task.getFullName()));
            }
            sb.append(String.format("Run the workflow again using `-s %s` option to retry this workflow.",
                        resumeResultPath));
            throw systemExit(sb.toString());
        }
        else if (sessionStatePath == null) {
            rsm.shutdown();
            resumeResultPath.delete();
        }
    }

    private Function<String, TaskReport> skipTaskReports = (fullName) -> null;

    public static class TaskRunnerManagerWithSkip
            extends TaskRunnerManager
    {
        private final TaskCallbackApi callback;
        private final ConfigFactory cf;
        private final Run cmd;

        @Inject
        public TaskRunnerManagerWithSkip(TaskCallbackApi callback, ConfigFactory cf,
                ConfigEvalEngine evalEngine, Set<TaskRunnerFactory> factories,
                Run cmd)
        {
            super(callback, cf, evalEngine, factories);
            this.callback = callback;
            this.cf = cf;
            this.cmd = cmd;
        }

        @Override
        public void run(TaskRequest request)
        {
            String fullName = request.getTaskInfo().getFullName();
            TaskReport report = cmd.skipTaskReports.apply(fullName);
            if (report != null) {
                try (SetThreadName threadName = new SetThreadName(fullName)) {
                    logger.info("Skipped");
                }
                callback.taskSucceeded(request.getTaskInfo().getId(),
                        cf.create(), cf.create(),
                        report);
            }
            else {
                super.run(request);
            }
        }
    }
}

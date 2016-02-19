package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDate;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.stream.Collectors;
import java.io.File;
import java.nio.file.Path;
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
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.agent.OperatorManager;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.agent.SetThreadName;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.ArchiveManager;
import io.digdag.core.agent.AgentId;
import io.digdag.core.agent.AgentConfig;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;
import static java.util.Locale.ENGLISH;

public class Run
    extends Command
{
    public static final String DEFAULT_DAGFILE = "digdag.yml";

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    @Parameter(names = {"-f", "--file"})
    String dagfilePath = null;

    @Parameter(names = {"-s", "--status"})
    String sessionStatusPath = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-t", "--session-time"})
    String sessionTime = null;

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    @Parameter(names = {"-e", "--show-params"})
    boolean showParams = false;

    @Parameter(names = {"-de"})
    boolean dryRunAndShowParams = false;

    //@Parameter(names = {"-G", "--graph"})
    //String visualizePath = null;

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
        if (dryRunAndShowParams) {
            dryRun = showParams = true;
        }

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
        System.err.println("    -s, --status DIR                 use this directory to read and write session status");
        System.err.println("    -p, --param KEY=VALUE            overwrite a parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read parameters from a YAML file");
        System.err.println("    -d, --dry-run                    dry-run mode doesn't execute tasks");
        System.err.println("    -e, --show-params                show task parameters before running a task");
        System.err.println("    -t, --session-time \"yyyy-MM-dd[ HH:mm:ss]\"  set session_time to this time");
        //System.err.println("    -g, --graph OUTPUT.png           visualize a task and exit");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private final DateTimeFormatter sessionTimeParser =
        DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]", ENGLISH);

    public void run(String taskNamePattern) throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(ResumeStateManager.class).in(Scopes.SINGLETON);
                binder.bind(YamlMapper.class).in(Scopes.SINGLETON);  // used by ResumeStateManager
                binder.bind(Run.class).toInstance(this);  // used by OperatorManagerWithSkip
            })
            .overrideModules((list) -> ImmutableList.of(Modules.override(list).with((binder) -> {
                binder.bind(OperatorManager.class).to(OperatorManagerWithSkip.class).in(Scopes.SINGLETON);
            })))
            .initialize()
            .getInjector();

        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.loadParameterizedFile(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        if (sessionStatusPath != null) {
            this.skipTaskReports = (fullName) -> rsm.readSuccessfulTaskReport(new File(sessionStatusPath), fullName);
        }

        Dagfile dagfile = loader.loadParameterizedFile(new File(dagfilePath), overwriteParams).convert(Dagfile.class);
        if (taskNamePattern == null) {
            if (dagfile.getDefaultTaskName().isPresent()) {
                taskNamePattern = dagfile.getDefaultTaskName().get();
            }
            else {
                throw new ConfigException(String.format(
                            "run: option is not written at %s file. Please add run: option or add +NAME option to command line", dagfilePath));
            }
        }

        StoredSessionAttemptWithSession attempt = localSite.storeAndStartLocalWorkflows(
                ArchiveMetadata.of(
                    dagfile.getWorkflowList(),
                    dagfile.getDefaultParams(),
                    dagfile.getDefaultTimeZone().or(ZoneId.systemDefault())),  // TODO should this systemDefault be configurable by cmdline argument?
                TaskMatchPattern.compile(taskNamePattern),
                overwriteParams,
                (sr, timeZone) -> {
                    Instant time;
                    if (sessionTime != null) {
                        TemporalAccessor parsed;
                        try {
                            parsed = sessionTimeParser
                                .withZone(timeZone)
                                .parse(sessionTime);
                        }
                        catch (DateTimeParseException ex) {
                            throw new ConfigException("-t, --session-time must be \"yyyy-MM-dd\" or \"yyyy-MM-dd HH:mm:SS\" format: " + sessionTime);
                        }
                        try {
                            time = Instant.from(parsed);
                        }
                        catch (DateTimeException ex) {
                            time = LocalDate.from(parsed)
                                .atStartOfDay(timeZone)
                                .toInstant();
                        }
                    }
                    else if (sr.isPresent()) {
                        time = sr.get().lastScheduleTime(Instant.now()).getTime();
                    }
                    else {
                        logger.warn("-t, --session-time at cmdline or schedule: parameter in yaml file is not set. Using current time as session_time.");
                        time = ScheduleTime.alignedNow();
                    }
                    return ScheduleTime.runNow(time);
                });
        logger.debug("Submitting {}", attempt);

        localSite.startLocalAgent();
        localSite.startMonitor();

        // if sessionStatusPath is not set, use workflow.yml.resume.yml
        File resumeResultPath = new File(Optional.fromNullable(sessionStatusPath).or("digdag.status"));
        rsm.startUpdate(resumeResultPath, attempt);

        localSite.runUntilAny();

        ArrayList<ArchivedTask> failedTasks = new ArrayList<>();
        for (ArchivedTask task : localSite.getSessionStore().getTasksOfAttempt(attempt.getId())) {
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
            logger.debug("    exported: "+task.getExportParams());
            logger.debug("    stored: "+task.getStoreParams());
            logger.debug("    stateParams: "+task.getStateParams());
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
            for (ArchivedTask task : failedTasks) {
                sb.append(String.format("  * %s:%n", task.getFullName()));
                String message = task.getError().get("message", String.class, "");
                if (message.isEmpty()) {
                    sb.append("    " + task.getError());
                }
                else {
                    int i = message.indexOf("Exception: ");
                    if (i > 0) {
                        message = message.substring(i + "Exception: ".length());
                    }
                    sb.append("    " + message);
                }
                sb.append(String.format("%n"));
            }
            sb.append(String.format("%n"));
            sb.append(String.format("Task state is stored at %s directory.%n", resumeResultPath));
            sb.append(String.format("Run the workflow again using `-s %s` option to retry this workflow.",
                        resumeResultPath));
            throw systemExit(sb.toString());
        }
        else if (sessionStatusPath == null) {
            rsm.shutdown();
            resumeResultPath.delete();
        }
    }

    private Function<String, TaskResult> skipTaskReports = (fullName) -> null;

    public static class OperatorManagerWithSkip
            extends OperatorManager
    {
        private final ConfigFactory cf;
        private final Run cmd;
        private final YamlMapper yamlMapper;

        @Inject
        public OperatorManagerWithSkip(
                AgentConfig config, AgentId agentId,
                TaskCallbackApi callback, ArchiveManager archiveManager,
                ConfigLoaderManager configLoader, WorkflowCompiler compiler, ConfigFactory cf,
                ConfigEvalEngine evalEngine, Set<OperatorFactory> factories,
                Run cmd, YamlMapper yamlMapper)
        {
            super(config, agentId, callback, archiveManager, configLoader, compiler, cf, evalEngine, factories);
            this.cf = cf;
            this.cmd = cmd;
            this.yamlMapper = yamlMapper;
        }

        @Override
        public void run(TaskRequest request)
        {
            String fullName = request.getTaskName();
            TaskResult result = cmd.skipTaskReports.apply(fullName);
            if (result != null) {
                try (SetThreadName threadName = new SetThreadName(fullName)) {
                    logger.info("Skipped");
                }
                callback.taskSucceeded(
                        request.getTaskId(), request.getLockId(), agentId,
                        result);
            }
            else {
                super.run(request);
            }
        }

        @Override
        protected TaskResult callExecutor(Path archivePath, String type, TaskRequest mergedRequest)
        {
            if (cmd.showParams) {
                StringBuilder sb = new StringBuilder();
                for (String line : yamlMapper.toYaml(mergedRequest.getConfig()).split("\n")) {
                    sb.append("  ").append(line).append("\n");
                }
                logger.warn("\n{}", sb.toString());
            }
            if (cmd.dryRun) {
                return TaskResult.empty(cf);
            }
            else {
                return super.callExecutor(archivePath, type, mergedRequest);
            }
        }
    }
}

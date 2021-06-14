package io.digdag.cli;

import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.Limits;
import io.digdag.core.LocalSite;
import io.digdag.core.LocalSite.StoreWorkflowResult;
import io.digdag.core.agent.AgentConfig;
import io.digdag.core.agent.AgentId;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.OperatorManager;
import io.digdag.core.agent.OperatorRegistry;
import io.digdag.core.agent.SetThreadName;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.log.LogModule;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskRelation;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.TaskTree;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.WorkflowTaskList;
import io.digdag.server.ac.DefaultAccessController;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.ac.AccessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Arguments.loadProject;
import static io.digdag.cli.Arguments.normalizeWorkflowName;
import static io.digdag.cli.SystemExitException.systemExit;
import static java.util.Locale.ENGLISH;

public class Run
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    @Parameter(names = {"--project"})
    String projectDirName = null;

    @Parameter(names = {"-a", "--rerun", "--all"})  // --all is kept here for backward compatibility but should be removed
    boolean rerunAll = false;

    @Parameter(names = {"-s", "--start"})
    String runStart = null;

    @Parameter(names = {"-g", "--goal"})
    String runStartStop = null;

    @Parameter(names = {"-e", "--end"})
    String runEnd = null;

    @Parameter(names = {"-o", "--save"})
    String sessionStatusDir = ".digdag/status";

    @Parameter(names = {"--no-save"})
    boolean noSave = false;

    @Parameter(names = {"-p", "--param"}, validateWith = ParameterValidator.class)
    List<String> paramsList = new ArrayList<>();
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-t", "--session-time", "--session"})  // --session-time is kept only for backward compatibility. should be removed at the next major release
    String sessionString = "last";

    @Parameter(names = {"-d", "--dry-run"})
    boolean dryRun = false;

    @Parameter(names = {"-E", "--show-params"})
    boolean showParams = false;

    // set system time zone. this doesn't overwrite dagfile.defaultTimeZone
    @Parameter(names = {"--timezone"})
    String timeZoneName = null;

    @Parameter(names = {"-dE"})
    boolean dryRunAndShowParams = false;

    @Parameter(names = {"--max-task-threads"})
    int maxTaskThreads = 0;

    @Parameter(names = {"-O", "--task-log"})
    String taskLogPath = null;

    private Path resumeStatePath;

    @Override
    public void main()
            throws Exception
    {
        JvmUtil.validateJavaRuntime(err);

        if (dryRunAndShowParams) {
            dryRun = showParams = true;
        }

        if (runStart != null && runStartStop != null) {
            throw usage("-s, --start and -g, --goal don't work together");
        }

        if (runStartStop != null && runEnd != null) {
            throw usage("-s, --start and -e, --end don't work together");
        }

        if (rerunAll && runStart != null) {
            throw usage("-a, --rerun and -s, --start don't work together");
        }

        if (rerunAll && runStartStop != null) {
            throw usage("-a, --rerun and -g, --goal don't work together");
        }

        switch (args.size()) {
        case 1:
            run(args.get(0), null);
            break;
        case 2:
            run(args.get(0), args.get(1));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " run <workflow.dig> [+task] [options...]");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -a, --rerun                      ignores status files saved at .digdag/status and re-runs all tasks");
        err.println("    -s, --start +NAME                runs this task and its following tasks even if their status files are stored at .digdag/status");
        err.println("    -g, --goal +NAME                 runs this task and its children tasks even if their status files are stored at .digdag/status");
        err.println("    -e, --end +NAME                  skips this task and its following tasks");
        err.println("    -o, --save DIR                   uses this directory to read and write status files (default: .digdag/status)");
        err.println("        --no-save                    doesn't save status files at .digdag/status");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        err.println("    -d, --dry-run                    dry-run mode doesn't execute tasks");
        err.println("    -E, --show-params                show task parameters before running a task");
        err.println("        --session <daily | hourly | schedule | last | \"yyyy-MM-dd[ HH:mm:ss]\">  set session_time to this time");
        err.println("                                     (default: last, reuses the latest session time stored at .digdag/status)");
        err.println("    --max-task-threads               Limit maximum number of task execution threads on the execution");
        err.println("    -O, --task-log DIR               store task logs to this path");
        Main.showCommonOptions(env, err);
        return systemExit(error);
    }

    private static final DateTimeFormatter SESSION_TIME_ARG_PARSER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]", ENGLISH);

    private static final DateTimeFormatter SESSION_DISPLAY_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

    // don't include \ / : * ? " < > | which are not usable on windows
    private static final DateTimeFormatter SESSION_STATE_TIME_DIRNAME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssxx", ENGLISH);

    private static final List<Long> USE_ALL = null;

    protected DigdagEmbed.Bootstrap setupBootstrap(Properties systemProps)
    {
        return new DigdagEmbed.Bootstrap()
                .setEnvironment(env)
                .setSystemConfig(PropertyUtils.toConfigElement(systemProps))
                .setSystemPlugins(loadSystemPlugins(systemProps))
                .addModules(binder -> {
                    Multibinder.newSetBinder(binder, SecretStore.class);
                    binder.bind(SecretStoreManager.class).to(LocalSecretStoreManager.class).in(Scopes.SINGLETON);
                    binder.bind(ResumeStateManager.class).in(Scopes.SINGLETON);
                    binder.bind(YamlMapper.class).in(Scopes.SINGLETON);  // used by ResumeStateManager
                    binder.bind(LogModule.class).in(Scopes.SINGLETON);
                    binder.bind(Run.class).toProvider(() -> this);  // used by OperatorManagerWithSkip
                    binder.bind(AccessController.class).to(DefaultAccessController.class);
                })
                .overrideModulesWith((binder) ->
                        binder.bind(OperatorManager.class).to(OperatorManagerWithSkip.class).in(Scopes.SINGLETON));
    }

    public void run(String workflowNameArg, String matchPattern)
            throws Exception
    {
        Properties systemProps = loadSystemProperties();

        if (maxTaskThreads > 0) {
            systemProps.setProperty("agent.max-task-threads", String.valueOf(maxTaskThreads));
        }

        if (taskLogPath != null) {
            systemProps.setProperty("log-server.type", "local");
            systemProps.setProperty("log-server.local.path", taskLogPath);
        }

        try (DigdagEmbed digdag = setupBootstrap(systemProps).initializeWithoutShutdownHook()) {
            run(systemProps, digdag.getInjector(), workflowNameArg, matchPattern);
        }
    }

    private void run(Properties systemProps, Injector injector, String workflowNameArg, String matchPattern)
            throws Exception
    {
        final LocalSite localSite = injector.getInstance(LocalSite.class);
        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final TransactionManager tm = injector.getInstance(TransactionManager.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        // read parameters
        params = ParameterValidator.toMap(paramsList);
        Config overrideParams = loadParams(cf, loader, systemProps, paramsFile, params);

        // load workflow definitions
        ProjectArchive project = loadProject(projectLoader, projectDirName, overrideParams);

        String workflowName = normalizeWorkflowName(project, workflowNameArg);

        Optional<TaskMatchPattern> taskMatchPattern;
        if (matchPattern == null || matchPattern.isEmpty()) {
            taskMatchPattern = Optional.absent();
        }
        else {
            taskMatchPattern = Optional.of(TaskMatchPattern.compile(matchPattern));
        }

        // WorkflowExecutor uses TransactionManager#begin by itself.
        // So other methods that access the database need to be wrapped by TransactionManager#begin.

        // store workflow definitions
        StoreWorkflowResult stored = tm.begin(() ->
                localSite.storeLocalWorkflowsWithoutSchedule(
                        "default",
                        Instant.now().toString(),  // TODO revision name
                        project.getArchiveMetadata()), ResourceConflictException.class);

        // submit workflow
        StoredSessionAttemptWithSession attempt = tm.begin(() ->
                submitWorkflow(injector,
                        stored.getRevision(), stored.getWorkflowDefinitions(),
                        project, overrideParams,
                        workflowName, taskMatchPattern), Exception.class); // Hmm... Too many exceptions...

        // TODO catch error when workflowName doesn't exist and suggest to cd to another dir

        // wait until it's done
        localSite.runUntilDone(attempt.getId());
        rsm.sync();

        // show results
        ArrayList<ArchivedTask> failedTasks = new ArrayList<>();
        List<ArchivedTask> tasks = tm.begin(() -> localSite.getSessionStore().getTasksOfAttempt(attempt.getId()));
        for (ArchivedTask task : tasks) {
            if (task.getState() == TaskStateCode.ERROR) {
                failedTasks.add(task);
            }
            logger.debug("  Task[" + task.getId() + "]: " + task.getFullName());
            logger.debug("    parent: " + task.getParentId().transform(it -> Long.toString(it)).or("(root)"));
            logger.debug("    upstreams: " + task.getUpstreams().stream().map(it -> Long.toString(it)).collect(Collectors.joining(",")));
            logger.debug("    state: " + task.getState());
            logger.debug("    retryAt: " + task.getRetryAt());
            logger.debug("    config: " + task.getConfig().getMerged());
            logger.debug("    taskType: " + task.getTaskType());
            logger.debug("    exported: " + task.getExportParams());
            logger.debug("    stored: " + task.getStoreParams());
            logger.debug("    stateParams: " + task.getStateParams());
            logger.debug("    in: " + task.getReport().transform(report -> report.getInputs()).or(ImmutableList.of()));
            logger.debug("    out: " + task.getReport().transform(report -> report.getOutputs()).or(ImmutableList.of()));
            logger.debug("    error: " + task.getError());
        }

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
            if (!noSave) {
                sb.append(String.format(ENGLISH, "Task state is saved at %s directory.%n", resumeStatePath));
                sb.append(String.format(ENGLISH, "  * Use --session <daily | hourly | \"yyyy-MM-dd[ HH:mm:ss]\"> to not reuse the last session time.%n"));
                sb.append(String.format(ENGLISH, "  * Use --rerun, --start +NAME, or --goal +NAME argument to rerun skipped tasks."));
            }
            throw systemExit(sb.toString());
        }
        else {
            if (noSave) {
                err.println(String.format(ENGLISH, "Success."));
            }
            else {
                err.println(String.format(ENGLISH, "Success. Task state is saved at %s directory.", resumeStatePath));
            }
            err.println(String.format(ENGLISH, "  * Use --session <daily | hourly | \"yyyy-MM-dd[ HH:mm:ss]\"> to not reuse the last session time."));
            err.println(String.format(ENGLISH, "  * Use --rerun, --start +NAME, or --goal +NAME argument to rerun skipped tasks."));
        }
    }

    private StoredSessionAttemptWithSession submitWorkflow(Injector injector,
            StoredRevision rev, List<StoredWorkflowDefinition> defs,
            ProjectArchive project, Config overrideParams,
            String workflowName, Optional<TaskMatchPattern> taskMatchPattern)
        throws SystemExitException, TaskMatchPattern.NoMatchException, TaskMatchPattern.MultipleTaskMatchException, ResourceNotFoundException, ResourceLimitExceededException, SessionAttemptConflictException
    {
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final WorkflowExecutor executor = injector.getInstance(WorkflowExecutor.class);
        final AttemptBuilder attemptBuilder = injector.getInstance(AttemptBuilder.class);
        final SchedulerManager srm = injector.getInstance(SchedulerManager.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        // compile the target workflow
        StoredWorkflowDefinition def = null;
        for (StoredWorkflowDefinition candidate : defs) {
            if (candidate.getName().equals(workflowName)) {
                def = candidate;
                break;
            }
        }
        if (def == null) {
            if (projectDirName == null) {
                if (workflowName.contains("/")) {
                    Path subdir = Paths.get(workflowName).getParent();
                    throw new ResourceNotFoundException(String.format(
                                "Workflow '%s' does not exist in current directory. You may need to type \"cd %s\" first, or set \"--project %s\" option.",
                                workflowName, subdir, subdir));
                }
                else {
                    throw new ResourceNotFoundException(String.format(
                                "Workflow '%s' does not exist in current directory. You may need to change directory first, or set --project option.",
                                workflowName, projectDirName));
                }
            }
            else {
                throw new ResourceNotFoundException(String.format(
                            "Workflow '%s' does not exist in project directory '%s'.",
                            workflowName, projectDirName));
            }
        }
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());

        // extract subtasks if necessary from the workflow
        WorkflowTaskList tasks;
        if (taskMatchPattern.isPresent()) {
            int fromIndex = taskMatchPattern.get().findIndex(workflow.getTasks());
            tasks = (fromIndex > 0) ?  // findIndex may return 0
                SubtreeExtract.extractSubtree(workflow.getTasks(), fromIndex) :
                workflow.getTasks();
        }
        else {
            tasks = workflow.getTasks();
        }

        // calculate ./.digdag/status/<session_time> path.
        // if sessionStatusDir is not set, use .digdag/status.
        Path sessionStatusPath = project.getProjectPath().resolve(sessionStatusDir).normalize();

        // calculate session_time
        Optional<Scheduler> sr = srm.tryGetScheduler(rev, def);
        ZoneId timeZone = def.getTimeZone();
        Instant sessionTime = parseSessionTime(sessionString, sessionStatusPath, def.getName(), sr, timeZone);

        this.resumeStatePath = sessionStatusPath.resolve(
                SESSION_STATE_TIME_DIRNAME_FORMATTER.withZone(timeZone).format(sessionTime)
                );
        if (!noSave) {
            logger.info("Using session {}.", resumeStatePath);
        }

        // process --rerun, --start, --goal, and --end options
        List<Long> resumeStateFileEnabledTaskIndexList = USE_ALL;
        List<Long> runTaskIndexList = USE_ALL;
        if (runStartStop != null) {
            // --goal
            long taskIndex = TaskMatchPattern.compile(runStartStop).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: resume
            // the others (tasks after this and this task): force run
            resumeStateFileEnabledTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdListFromFar(taskIndex)
                    );
            // tasks before this: run
            // children tasks: run
            // this task: run
            // the others (tasks after this): skip
            runTaskIndexList = new ArrayList<>();
            runTaskIndexList.addAll(taskTree.getRecursiveParentsUpstreamChildrenIdListFromFar(taskIndex));
            runTaskIndexList.addAll(taskTree.getRecursiveChildrenIdList(taskIndex));
            runTaskIndexList.add(taskIndex);
        }
        if (runStart != null) {
            long startIndex = TaskMatchPattern.compile(runStart).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: resume
            // the others (tasks after this and this tasks): force run
            resumeStateFileEnabledTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdListFromFar(startIndex)
                    );
        }
        if (rerunAll) {
            // all tasks: force run
            resumeStateFileEnabledTaskIndexList = ImmutableList.of();
        }
        if (runEnd != null) {
            long endIndex = TaskMatchPattern.compile(runEnd).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: run
            // the others (tasks after this and this task): skip
            runTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdListFromFar(endIndex)
                    );
        }

        Set<String> resumeStateFileEnabledTaskNames =
            (resumeStateFileEnabledTaskIndexList == USE_ALL)
            ? null
            : ImmutableSet.copyOf(
                    resumeStateFileEnabledTaskIndexList.stream()
                    .map(index -> tasks.get(index.intValue()).getFullName())
                    .collect(Collectors.toList()));

        Set<String> runTaskNames =
            (runTaskIndexList == USE_ALL)
            ? null
            : ImmutableSet.copyOf(
                    runTaskIndexList.stream()
                    .map(index -> tasks.get(index.intValue()).getFullName())
                    .collect(Collectors.toList()));

        this.skipTaskReports = (fullName) -> {
            if (noSave) {
                return (TaskResult) null;
            }
            else {
                if (runTaskNames != null && !runTaskNames.contains(fullName)) {
                    return TaskResult.empty(overrideParams.getFactory());
                }
                else if (resumeStateFileEnabledTaskNames == null || resumeStateFileEnabledTaskNames.contains(fullName)) {
                    return rsm.readSuccessfulTaskReport(resumeStatePath, fullName);
                }
                else {
                    return (TaskResult) null;
                }
            }
        };

        AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                rev,
                def,
                overrideParams,
                ScheduleTime.runNow(sessionTime));

        StoredSessionAttemptWithSession attempt = executor.submitTasks(0, ar, tasks);
        logger.debug("Submitting {}", attempt);

        if (!noSave) {
            rsm.startUpdate(resumeStatePath, attempt);
        }

        return attempt;
    }

    private static TaskTree makeIndexTaskTree(WorkflowTaskList tasks)
    {
        List<TaskRelation> relations = tasks.stream()
            .map(t -> TaskRelation.of(
                        t.getIndex(),
                        Optional.fromNullable(t.getParentIndex().transform(it -> it.longValue()).orNull()),
                        t.getUpstreamIndexes().stream().map(it -> it.longValue()).collect(Collectors.toList())
                        ))
            .collect(Collectors.toList());
        return new TaskTree(relations);
    }

    private static Instant parseSessionTime(String sessionString,
            Path sessionStatusPath, String workflowName,
            Optional<Scheduler> sr, ZoneId timeZone)
        throws SystemExitException
    {
        switch (sessionString) {
        case "hourly":
            return ZonedDateTime.ofInstant(Instant.now(), timeZone)
                .truncatedTo(ChronoUnit.HOURS)
                .toInstant();

        case "daily":
            return ZonedDateTime.ofInstant(Instant.now(), timeZone)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant();

        case "last":
            {
                Optional<Instant> last = getLastSessionTime(sessionStatusPath, workflowName);
                if (last.isPresent()) {
                    logger.warn("Reusing the last session time {}.", SESSION_DISPLAY_FORMATTER.withZone(timeZone).format(last.get()));
                    return last.get();
                }
                else if (sr.isPresent()) {
                    Instant t = sr.get().lastScheduleTime(Instant.now()).getTime();
                    logger.warn("Using a new session time {} based on schedule.", SESSION_DISPLAY_FORMATTER.withZone(timeZone).format(t));
                    return t;
                }
                else {
                    Instant t = ZonedDateTime.ofInstant(Instant.now(), timeZone)
                        .truncatedTo(ChronoUnit.DAYS)
                        .toInstant();
                    logger.warn("Using a new session time {}.", SESSION_DISPLAY_FORMATTER.withZone(timeZone).format(t));
                    return t;
                }
            }

        case "schedule":
            if (sr.isPresent()) {
                return sr.get().lastScheduleTime(Instant.now()).getTime();
            }
            throw systemExit("--session schedule is set but schedule is not set to this workflow.");

        default:
            TemporalAccessor parsed;
            try {
                parsed = SESSION_TIME_ARG_PARSER
                    .withZone(timeZone)
                    .parse(sessionString);
            }
            catch (DateTimeParseException ex) {
                throw new ConfigException("--session must be hourly, daily, last, \"yyyy-MM-dd\", or \"yyyy-MM-dd HH:mm:SS\" format: " + sessionString);
            }
            try {
                return Instant.from(parsed);
            }
            catch (DateTimeException ex) {
                return LocalDate.from(parsed)
                    .atStartOfDay(timeZone)
                    .toInstant();
            }
        }
    }

    private static Optional<Instant> getLastSessionTime(Path sessionStatusPath, String workflowName)
    {
        try {
            List<Instant> times = new ArrayList<>();
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(sessionStatusPath, p -> Files.isDirectory(p) && taskExists(p, workflowName))) {
                for (Path path : ds) {
                    try {
                        times.add(Instant.from(SESSION_STATE_TIME_DIRNAME_FORMATTER.parse(path.getFileName().toString())));
                    }
                    catch (DateTimeException ex) {
                    }
                }
            }
            if (times.isEmpty()) {
                return Optional.absent();
            }
            else {
                return Optional.of(times.stream().sorted(Comparator.<Instant>naturalOrder().reversed()).findFirst().get());
            }
        }
        catch (NoSuchFileException ex) {
            return Optional.absent();
        }
        catch (IOException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private static boolean taskExists(Path dir, String workflowName)
        throws IOException
    {
        Pattern namePattern = Pattern.compile(Pattern.quote("+" + workflowName) + "[\\+\\^].*\\.yml");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, f -> Files.isRegularFile(f))) {
            for (Path file : ds) {
                if (namePattern.matcher(file.getFileName().toString()).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    private Function<String, TaskResult> skipTaskReports = (fullName) -> null;

    private static class OperatorManagerWithSkip
            extends OperatorManager
    {
        private final ConfigFactory cf;
        private final Run cmd;
        private final YamlMapper yamlMapper;

        @Inject
        private OperatorManagerWithSkip(
                AgentConfig config, AgentId agentId,
                TaskCallbackApi callback, WorkspaceManager workspaceManager,
                ConfigFactory cf,
                ConfigEvalEngine evalEngine, OperatorRegistry registry,
                Run cmd, YamlMapper yamlMapper,
                SecretStoreManager secretStoreManager, Limits limits)
        {
            super(config, agentId, callback, workspaceManager, cf, evalEngine, registry, secretStoreManager, limits);
            this.cf = cf;
            this.cmd = cmd;
            this.yamlMapper = yamlMapper;
        }

        @Override
        public void run(TaskRequest request)
        {
            String fullName = request.getTaskName();
            TaskResult result = cmd.skipTaskReports.apply(fullName);
            String origThreadName = String.format("[%d:%s]%s", request.getSiteId(), request.getProjectName().or("----"), request.getTaskName());
            if (result != null) {
                try (SetThreadName threadName = new SetThreadName(origThreadName)) {
                    logger.warn("Skipped");
                }
                callback.taskSucceeded(request, agentId, result);
            }
            else {
                super.run(request);
            }
        }

        @Override
        protected TaskResult callExecutor(Path workspacePath, String type, TaskRequest mergedRequest)
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
                return super.callExecutor(workspacePath, type, mergedRequest);
            }
        }
    }
}

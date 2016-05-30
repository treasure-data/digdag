package io.digdag.cli;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.ZonedDateTime;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.LocalSite.StoreWorkflowResult;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskRelation;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.agent.OperatorManager;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.agent.SetThreadName;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.agent.AgentId;
import io.digdag.core.agent.AgentConfig;
import io.digdag.core.workflow.TaskTree;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTaskList;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
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

    @DynamicParameter(names = {"-p", "--param"})
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

    private Path resumeStatePath;

    public Run(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

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
        err.println("Usage: digdag run <workflow.dig> [+task] [options...]");
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
        Main.showCommonOptions(err);
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

    public void run(String workflowNameArg, String matchPattern) throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(ResumeStateManager.class).in(Scopes.SINGLETON);
                binder.bind(YamlMapper.class).in(Scopes.SINGLETON);  // used by ResumeStateManager
                binder.bind(Run.class).toInstance(this);  // used by OperatorManagerWithSkip
            })
            .overrideModulesWith((binder) -> {
                binder.bind(OperatorManager.class).to(OperatorManagerWithSkip.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final LocalSite localSite = injector.getInstance(LocalSite.class);
        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        // read parameters
        Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

        // load workflow definitions
        ProjectArchive project = loadProject(projectLoader, projectDirName, overwriteParams);

        String workflowName = normalizeWorkflowName(project, workflowNameArg);

        Optional<TaskMatchPattern> taskMatchPattern;
        if (matchPattern == null || matchPattern.isEmpty()) {
            taskMatchPattern = Optional.absent();
        }
        else {
            taskMatchPattern = Optional.of(TaskMatchPattern.compile(matchPattern));
        }

        // store workflow definition archive
        ArchiveMetadata archive = project.getArchiveMetadata();
        StoreWorkflowResult stored = localSite.storeLocalWorkflowsWithoutSchedule(
                "default",
                Instant.now().toString(),  // TODO revision name
                archive);

        // submit workflow
        StoredSessionAttemptWithSession attempt = submitWorkflow(injector,
                stored.getRevision(), stored.getWorkflowDefinitions(),
                archive, overwriteParams,
                workflowName, taskMatchPattern);
        // TODO catch error when workflowName doesn't exist and suggest to cd to another dir

        // wait until it's done
        localSite.runUntilDone(attempt.getId());
        rsm.sync();

        // show results
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
            logger.debug("    config: "+task.getConfig().getMerged());
            logger.debug("    taskType: "+task.getTaskType());
            logger.debug("    exported: "+task.getExportParams());
            logger.debug("    stored: "+task.getStoreParams());
            logger.debug("    stateParams: "+task.getStateParams());
            logger.debug("    in: "+task.getReport().transform(report -> report.getInputs()).or(ImmutableList.of()));
            logger.debug("    out: "+task.getReport().transform(report -> report.getOutputs()).or(ImmutableList.of()));
            logger.debug("    error: "+task.getError());
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
            ArchiveMetadata archive, Config overwriteParams,
            String workflowName, Optional<TaskMatchPattern> taskMatchPattern)
        throws SystemExitException, TaskMatchPattern.NoMatchException, TaskMatchPattern.MultipleTaskMatchException, ResourceNotFoundException, SessionAttemptConflictException
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
                                "Workflow '%s' doesn't not exist in current directory. You may need to type \"cd %s\" first, or set \"--project %s\" option.",
                                workflowName, subdir, subdir));
                }
                else {
                    throw new ResourceNotFoundException(String.format(
                                "Workflow '%s' doesn't not exist in current directory. You may need to change directory first, or set --project option.",
                                workflowName, projectDirName));
                }
            }
            else {
                throw new ResourceNotFoundException(String.format(
                            "Workflow '%s' doesn't not exist in project directory '%s'.",
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

        // calculate session_time
        Optional<Scheduler> sr = srm.tryGetScheduler(rev, def);
        ZoneId timeZone = def.getTimeZone();
        Instant sessionTime = parseSessionTime(sessionString, Paths.get(sessionStatusDir), def.getName(), sr, timeZone);

        // calculate ./.digdag/status/<session_time> path.
        // if sessionStatusDir is not set, use .digdag/status.
        this.resumeStatePath = Paths.get(sessionStatusDir).resolve(
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
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(taskIndex)
                    );
            // tasks before this: run
            // children tasks: run
            // this task: run
            // the others (tasks after this): skip
            runTaskIndexList = new ArrayList<>();
            runTaskIndexList.addAll(taskTree.getRecursiveParentsUpstreamChildrenIdList(taskIndex));
            runTaskIndexList.addAll(taskTree.getRecursiveChildrenIdList(taskIndex));
            runTaskIndexList.add(taskIndex);
        }
        if (runStart != null) {
            long startIndex = TaskMatchPattern.compile(runStart).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: resume
            // the others (tasks after this and this tasks): force run
            resumeStateFileEnabledTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(startIndex)
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
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(endIndex)
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
                    return TaskResult.empty(overwriteParams.getFactory());
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
                overwriteParams,
                ScheduleTime.runNow(sessionTime),
                Optional.absent());

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
            Path sessionStatusDir, String workflowName,
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
                Optional<Instant> last = getLastSessionTime(sessionStatusDir, workflowName);
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

    private static Optional<Instant> getLastSessionTime(Path sessionStatusDir, String workflowName)
    {
        try {
            List<Instant> times = new ArrayList<>();
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(sessionStatusDir, p -> Files.isDirectory(p) && taskExists(p, workflowName))) {
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
            throw Throwables.propagate(ex);
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
                WorkflowCompiler compiler, ConfigFactory cf,
                ConfigEvalEngine evalEngine, Set<OperatorFactory> factories,
                Run cmd, YamlMapper yamlMapper)
        {
            super(config, agentId, callback, workspaceManager, compiler, cf, evalEngine, factories);
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
                    logger.warn("Skipped");
                }
                callback.taskSucceeded(request.getSiteId(),
                        request.getTaskId(), request.getLockId(), agentId,
                        result);
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

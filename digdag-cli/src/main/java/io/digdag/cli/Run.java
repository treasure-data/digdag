package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.EnumSet;
import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
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
import java.io.File;
import java.io.IOException;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.LocalSite.StoreWorkflowResult;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskRelation;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.agent.OperatorManager;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.agent.SetThreadName;
import io.digdag.core.agent.ConfigEvalEngine;
import io.digdag.core.agent.ArchiveManager;
import io.digdag.core.agent.AgentId;
import io.digdag.core.agent.AgentConfig;
import io.digdag.core.workflow.TaskTree;
import io.digdag.core.workflow.SubtaskExtract;
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
import static io.digdag.cli.Main.systemExit;
import static java.util.Locale.ENGLISH;

public class Run
    extends Command
{
    public static final String DEFAULT_DAGFILE = "digdag.yml";

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    @Parameter(names = {"-f", "--file"})
    String dagfilePath = null;

    @Parameter(names = {"-a", "--all"})
    boolean runAll = false;

    @Parameter(names = {"-s", "--start"})
    String runStart = null;

    @Parameter(names = {"-g", "--goal"})
    String runStartStop = null;

    @Parameter(names = {"-e", "--end"})
    String runEnd = null;

    @Parameter(names = {"-o", "--save"})
    String sessionStatusDir = "digdag.status";

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

    private boolean runAsImplicit = false;

    private Path resumeStatePath;

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

        if (runStart != null && runStartStop != null) {
            throw usage("-s, --start and -g, --goal don't work together");
        }

        if (runStartStop != null && runEnd != null) {
            throw usage("-s, --start and -e, --end don't work together");
        }

        if (runAll && runStart != null) {
            throw usage("-a, --all and -s, --start don't work together");
        }

        if (runAll && runStartStop != null) {
            throw usage("-a, --all and -g, --goal don't work together");
        }

        String taskNamePattern;
        switch (args.size()) {
        case 0:
            taskNamePattern = null;
            break;
        case 1:
            taskNamePattern = args.get(0);
            if (!taskNamePattern.startsWith("+")) {
                throw usage("Task name '" + taskNamePattern + "' does not exist in file " + dagfilePath + ".");
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
        System.err.println("    -f, --file PATH.yml              use this file to load tasks (default: digdag.yml)");
        System.err.println("    -a, --all                        ignores status files saved at digdag.status and runs all tasks");
        System.err.println("    -s, --start +NAME                runs this task and its following tasks even if their status files are stored at digdag.status");
        System.err.println("    -g, --goal +NAME                 runs this task and its children tasks even if their status files are stored at digdag.status");
        System.err.println("    -e, --end +NAME                  skips this task and its following tasks");
        System.err.println("    -o, --save DIR                   uses this directory to read and write status files (default: digdag.status)");
        System.err.println("        --no-save                    doesn't save status files at digdag.status");
        System.err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        System.err.println("    -d, --dry-run                    dry-run mode doesn't execute tasks");
        System.err.println("    -E, --show-params                show task parameters before running a task");
        System.err.println("        --session <daily | hourly | schedule | last | \"yyyy-MM-dd[ HH:mm:ss]\">  set session_time to this time");
        System.err.println("                                     (default: last, reuses the latest session time stored at digdag.status)");
        Main.showCommonOptions();
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

    public void run(String taskNamePattern) throws Exception
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

        // read parameters
        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.merge(loader.loadParameterizedFile(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        // read workflow definitions
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
        TaskMatchPattern taskMatchPattern = TaskMatchPattern.compile(taskNamePattern);

        // store workflow definition archive
        ArchiveMetadata archive = ArchiveMetadata.of(
                dagfile.getWorkflowList(),
                dagfile.getDefaultParams(),
                dagfile.getDefaultTimeZone().or(
                    Optional.fromNullable(timeZoneName).transform(it -> ZoneId.of(it)).or(ZoneId.systemDefault()))
                );
        StoreWorkflowResult stored = localSite.storeLocalWorkflows(
                "default",
                Instant.now().toString(),  // TODO name
                archive,
                Optional.absent());  // Optional.absent to disable workflow scheduling

        // submit workflow
        StoredSessionAttemptWithSession attempt = submitWorkflow(injector,
                stored.getRevision(), stored.getWorkflowDefinitions(),
                archive, overwriteParams, taskMatchPattern);

        // wait until it's done
        localSite.runUntilAny();

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
                sb.append(String.format(ENGLISH, "Run command with --session '%s' argument to retry failed tasks.",
                            SESSION_TIME_ARG_PARSER.withZone(attempt.getTimeZone()).format(attempt.getSession().getSessionTime())));
            }
            throw systemExit(sb.toString());
        }
        else {
            if (noSave) {
                System.err.println(String.format(ENGLISH, "Success."));
            }
            else {
                System.err.println(String.format(ENGLISH, "Success. Task state is saved at %s directory.", resumeStatePath));
            }
            System.err.println(String.format(ENGLISH, "  * Use --session <daily | hourly | \"yyyy-MM-dd[ HH:mm:ss]\"> to not reuse the last session time."));
            System.err.println(String.format(ENGLISH, "  * Use --all, --start +NAME, or --goal +NAME argument to rerun skipped tasks."));
        }
    }

    private StoredSessionAttemptWithSession submitWorkflow(Injector injector,
            StoredRevision rev, List<StoredWorkflowDefinition> defs,
            ArchiveMetadata archive, Config overwriteParams, TaskMatchPattern taskMatchPattern)
        throws SystemExitException, TaskMatchPattern.NoMatchException, TaskMatchPattern.MultipleTaskMatchException, SessionAttemptConflictException
    {
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final WorkflowExecutor executor = injector.getInstance(WorkflowExecutor.class);
        final AttemptBuilder attemptBuilder = injector.getInstance(AttemptBuilder.class);
        final SchedulerManager srm = injector.getInstance(SchedulerManager.class);
        final ResumeStateManager rsm = injector.getInstance(ResumeStateManager.class);

        // compile the target workflow
        StoredWorkflowDefinition def = taskMatchPattern.findRootWorkflow(defs);
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());

        // extract subtasks if necessary from the workflow
        WorkflowTaskList tasks;
        Optional<SubtaskMatchPattern> subtaskPattern = taskMatchPattern.getSubtaskMatchPattern();
        if (subtaskPattern.isPresent()) {
            int fromIndex = subtaskPattern.get().findIndex(workflow.getTasks());
            tasks = (fromIndex > 0) ?  // findIndex may return 0
                SubtaskExtract.extract(workflow.getTasks(), fromIndex) :
                workflow.getTasks();
        }
        else {
            tasks = workflow.getTasks();
        }

        // calculate session_time
        Optional<Scheduler> sr = srm.tryGetScheduler(rev, def);
        ZoneId timeZone = sr.transform(it -> it.getTimeZone()).or(archive.getDefaultTimeZone());
        Instant sessionTime = parseSessionTime(sessionString, Paths.get(sessionStatusDir), def.getName(), sr, timeZone);

        // calculate ./digdag.status/<session_time> path.
        // if sessionStatusDir is not set, use digdag.status.
        this.resumeStatePath = Paths.get(sessionStatusDir).resolve(
                SESSION_STATE_TIME_DIRNAME_FORMATTER.withZone(timeZone).format(sessionTime)
                );
        if (!noSave) {
            logger.info("Using session {}.", resumeStatePath);
        }

        // process --all, --start, --goal, and --end options
        List<Long> resumeStateFileEnabledTaskIndexList = USE_ALL;
        List<Long> runTaskIndexList = USE_ALL;
        if (runStartStop != null) {
            // --goal
            long taskIndex = SubtaskMatchPattern.compile(runStartStop).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: resume
            // the others (tasks after this and this task): force run
            resumeStateFileEnabledTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(taskIndex)
                    );
            resumeStateFileEnabledTaskIndexList.remove(taskIndex);
            // tasks before this: run
            // children tasks: run
            // this task: run
            // the others (tasks after this): skip
            runTaskIndexList = ImmutableList.copyOf(Iterables.concat(
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(taskIndex),
                    taskTree.getRecursiveChildrenIdList(taskIndex)
                    ));
        }
        if (runStart != null) {
            long startIndex = SubtaskMatchPattern.compile(runStart).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: resume
            // the others (tasks after this and this tasks): force run
            resumeStateFileEnabledTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(startIndex)
                    );
            resumeStateFileEnabledTaskIndexList.remove(startIndex);
        }
        if (runAll) {
            // all tasks: force run
            resumeStateFileEnabledTaskIndexList = ImmutableList.of();
        }
        if (runEnd != null) {
            long endIndex = SubtaskMatchPattern.compile(runEnd).findIndex(tasks);
            TaskTree taskTree = makeIndexTaskTree(tasks);
            // tasks before this: run
            // the others (tasks after this and this task): skip
            runTaskIndexList = new ArrayList<>(
                    taskTree.getRecursiveParentsUpstreamChildrenIdList(endIndex)
                    );
            runTaskIndexList.remove(endIndex);
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
                Optional.absent(),
                rev,
                def,
                overwriteParams,
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
                    logger.warn("Using a new session time {} based on _schedule.", SESSION_DISPLAY_FORMATTER.withZone(timeZone).format(t));
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
            throw systemExit("--session schedule is set but _schedule is not set to this workflow.");

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
            for (Path path : Files.newDirectoryStream(sessionStatusDir, path -> Files.isDirectory(path) && taskExists(path, workflowName))) {
                try {
                    times.add(Instant.from(SESSION_STATE_TIME_DIRNAME_FORMATTER.parse(path.getFileName().toString())));
                }
                catch (DateTimeException ex) {
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
        Pattern namePattern = Pattern.compile(Pattern.quote(workflowName) + "(?:[\\+\\^].*\\.yml|\\.yml)");
        for (Path file : Files.newDirectoryStream(dir, file -> Files.isRegularFile(file))) {
            if (namePattern.matcher(file.getFileName().toString()).matches()) {
                return true;
            }
        }
        return false;
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

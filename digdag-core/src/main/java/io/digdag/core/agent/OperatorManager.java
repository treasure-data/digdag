package io.digdag.core.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.JsonNode;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Limits;
import io.digdag.core.log.LogLevel;
import io.digdag.core.log.LogMarkers;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.TaskLogger;
import io.digdag.core.ErrorReporter;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateException;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.spi.metrics.DigdagMetrics.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.util.Locale.ENGLISH;

public class OperatorManager
{
    private static Logger logger = LoggerFactory.getLogger(OperatorManager.class);

    protected final AgentConfig agentConfig;
    protected final AgentId agentId;
    protected final TaskCallbackApi callback;
    private final WorkspaceManager workspaceManager;
    private final ConfigFactory cf;
    private final ConfigEvalEngine evalEngine;
    private final OperatorRegistry registry;
    private final SecretStoreManager secretStoreManager;

    private final ScheduledExecutorService heartbeatScheduler;
    private final ConcurrentHashMap<Long, TaskRequest> runningTaskMap = new ConcurrentHashMap<>();  // {taskId => TaskRequest}

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    private final Limits limits;

    @Inject
    public OperatorManager(AgentConfig agentConfig, AgentId agentId,
            TaskCallbackApi callback, WorkspaceManager workspaceManager,
            ConfigFactory cf,
            ConfigEvalEngine evalEngine, OperatorRegistry registry,
            SecretStoreManager secretStoreManager, Limits limits)
    {
        this.agentConfig = agentConfig;
        this.agentId = agentId;
        this.callback = callback;
        this.workspaceManager = workspaceManager;
        this.cf = cf;
        this.evalEngine = evalEngine;
        this.registry = registry;
        this.secretStoreManager = secretStoreManager;
        this.limits = limits;

        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("heartbeat-%d")
                .build()
                );
    }

    @PostConstruct
    public void start()
    {
        heartbeatScheduler.scheduleAtFixedRate(() -> heartbeat(),
                agentConfig.getHeartbeatInterval(), agentConfig.getHeartbeatInterval(),
                TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        heartbeatScheduler.shutdown();
        // TODO wait for shutdown completion?
    }

    @DigdagTimed(value = "opm_", category = "agent", appendMethodName = true)
    public void run(TaskRequest request)
    {
        long taskId = request.getTaskId();
        String origThreadName = String.format("[%d:%s]%s", request.getSiteId(), request.getProjectName().or("----"), request.getTaskName());

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(origThreadName)) {
            try (TaskLogger taskLogger = callback.newTaskLogger(request)) {
                TaskContextLogging.enter(LogLevel.DEBUG, taskLogger);
                try {
                    runningTaskMap.put(taskId, request);
                    try {
                        runWithHeartbeat(request);
                    }
                    finally {
                        runningTaskMap.remove(taskId);
                    }
                }
                finally {
                    TaskContextLogging.leave();
                }
            }
        }
    }

    @DigdagTimed(value = "opm_", category = "agent", appendMethodName = true)
    protected void runWithHeartbeat(TaskRequest request)
    {
        try {
            workspaceManager.withExtractedArchive(request, () -> callback.openArchive(request), (projectPath) -> {
                try {
                    runWithWorkspace(projectPath, request);
                }
                catch (TaskExecutionException ex) {
                    if (ex.getRetryInterval().isPresent()) {
                        if (!ex.getError(cf).isPresent()) {
                            logger.info("Retrying task {}", ex.toString());
                        }
                        else {
                            logger.error("Task failed, retrying", ex);
                        }
                        callback.retryTask(request, agentId, ex.getRetryInterval().get(), ex.getStateParams(cf).get(), ex.getError(cf));
                    }
                    else {
                        if (request.isCancelRequested()) {
                            logger.warn("Task {} is canceled.", request.getTaskName());
                        }
                        else {
                            logger.error("Task {} failed.\n{}", request.getTaskName(), formatExceptionMessage(ex));
                            logger.info("", ex);
                        }
                        // TODO use debug to log stacktrace here
                        callback.taskFailed(request, agentId, ex.getError(cf).get());  // TODO is error set?
                    }
                }
                catch (RuntimeException | AssertionError ex) { // Avoid infinite task retry cause of AssertionError by Operators
                    if (ex instanceof ConfigException) {
                        logger.error("Configuration error at task {}: {}", request.getTaskName(), formatExceptionMessage(ex));
                    }
                    else {
                        logger.error(
                                LogMarkers.UNEXPECTED_SERVER_ERROR,
                                "Task failed with unexpected error: {}", ex.getMessage(), ex);
                    }
                    callback.taskFailed(request, agentId, buildExceptionErrorConfig(ex).toConfig(cf));  // no retry
                }
                catch (Throwable ex) {
                    logger.error(
                            LogMarkers.UNEXPECTED_SERVER_ERROR,
                            "Task failed with unexpected error: {}", ex.getMessage(), ex);

                    // This block catches OutOfMemoryError and its cause can be either deterministic or non-deterministic.
                    // So, OperatorManager can't always cancel the failed task.
                    if (request.isCancelRequested()) {
                        logger.warn("This task will be canceled since it's already requested to be canceled");
                        // This call cancels a task if it's requested to be canceled
                        callback.taskFailed(request, agentId, buildExceptionErrorConfig(ex).toConfig(cf));
                    }
                }
                return true;
            });
        }
        catch (RuntimeException | IOException ex) {
            // exception happened in workspaceManager
            logger.error(
                    LogMarkers.UNEXPECTED_SERVER_ERROR,
                    "Task failed with unexpected error: {}", ex.getMessage(), ex);
            callback.taskFailed(request, agentId, buildExceptionErrorConfig(ex).toConfig(cf));
        }
    }

    @DigdagTimed(value = "opm_", category = "agent", appendMethodName = true)
    protected Config evalConfig(TaskRequest request)
            throws RuntimeException, AssertionError
    {
        try {
            Config all = cf.create();
            all.merge(request.getConfig());  // export / carry params (TaskRequest.config sent by WorkflowExecutor doesn't include config of this task)
            Config runtimeParams = RuntimeParams.buildRuntimeParams(request.getConfig().getFactory(), request).deepCopy();
            all.merge(runtimeParams); //runtime parameter should not be override request parameters

            Config evalParams = all.deepCopy();
            all.merge(request.getLocalConfig());

            //ToDo get metrics on parameter size
            return evalEngine.eval(all, evalParams);
        }
        catch (TemplateException te) {
            throw new ConfigException(te.getMessage(), te);
        }
    }

    @DigdagTimed(value = "opm_", category = "agent", appendMethodName = true)
    protected void runWithWorkspace(Path projectPath, TaskRequest request)
        throws TaskExecutionException
    {
        // evaluate config and creates the complete merged config.
        Config config;
        try {
            config = evalConfig(request);
        }
        catch (ConfigException ex) {
            throw ex;
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Failed to process variables", ex);
        }
        catch (AssertionError ex) { // Avoid infinite task retry cause of AssertionError by ConfigEvalEngine(nashorn)
            throw new RuntimeException("Unexpected error happened in ConfigEvalEngine: " + ex.getMessage(), ex);
        }
        logger.debug("evaluated config: {}", filterConfigForLogging(config));

        Set<String> shouldBeUsedKeys = new HashSet<>(request.getLocalConfig().getKeys());

        String type;
        if (config.has("_type")) {
            type = config.get("_type", String.class);
            logger.info("type: {}", type);
            shouldBeUsedKeys.remove("_type");
        }
        else {
            java.util.Optional<String> operatorKey = config.getKeys()
                .stream()
                .filter(key -> key.endsWith(">"))
                .findFirst();
            if (!operatorKey.isPresent()) {
                // TODO warning
                callback.taskSucceeded(request, agentId, TaskResult.empty(cf));


                return;
            }
            type = operatorKey.get().substring(0, operatorKey.get().length() - 1);
            Object command = config.getOptional(operatorKey.get(), Object.class).orNull();
            config.set("_type", type);
            config.set("_command", command);
            logger.info("{}>: {}", type, Optional.fromNullable(command).or(""));
            shouldBeUsedKeys.remove(operatorKey.get());
        }

        // Now config is evaluated using JavaScript. Copy values from it
        // to create evaluated localConfig (original localConfig is not
        // evaluated).

        Config localConfig = config.getFactory().create();
        for (String localKey : request.getLocalConfig().getKeys()) {
            localConfig.set(localKey, config.getOptional(localKey, JsonNode.class).transform(JsonNode::deepCopy).orNull());
        }

        // Track accessed keys using UsedKeysSet class
        CheckedConfig.UsedKeysSet usedKeys = new CheckedConfig.UsedKeysSet();

        TaskRequest mergedRequest = TaskRequest.builder()
            .from(request)
            .localConfig(new CheckedConfig(localConfig, usedKeys))
            .config(new CheckedConfig(config, usedKeys))
            .build();

        TaskResult result = callExecutor(projectPath, type, mergedRequest);

        if (!usedKeys.isAllUsed()) {
            shouldBeUsedKeys.removeAll(usedKeys);
            if (!shouldBeUsedKeys.isEmpty()) {
                warnUnusedKeys(request, shouldBeUsedKeys, usedKeys);
            }
        }

        callback.taskSucceeded(request, agentId, result);
    }

    private void warnUnusedKeys(TaskRequest request, Set<String> shouldBeUsedButNotUsedKeys, Collection<String> candidateKeys)
    {
        for (String key : shouldBeUsedButNotUsedKeys) {
            logger.error("Parameter '{}' is not used at task {}.", key, request.getTaskName());
            List<String> suggestions = EditDistance.suggest(key, candidateKeys, 0.50);
            if (!suggestions.isEmpty()) {
                logger.error("  > Did you mean {}?", suggestions);
            }
        }
    }

    @DigdagTimed(value = "opm_", category = "agent", appendMethodName = true)
    protected TaskResult callExecutor(Path projectPath, String type, TaskRequest mergedRequest)
    {
        OperatorFactory factory = registry.get(mergedRequest, type);
        if (factory == null) {
            throw new ConfigException("Unknown task type: " + type);
        }

        SecretStore secretStore = secretStoreManager.getSecretStore(mergedRequest.getSiteId());

        SecretAccessContext secretContext = SecretAccessContext.builder()
                .siteId(mergedRequest.getSiteId())
                .projectId(mergedRequest.getProjectId())
                .revision(mergedRequest.getRevision().get())
                .workflowName(mergedRequest.getWorkflowName())
                .taskName(mergedRequest.getTaskName())
                .operatorType(type)
                .build();

        // Users can mount secrets
        Config secretMounts = mergedRequest.getConfig().getNestedOrGetEmpty("_secrets");
        mergedRequest.getConfig().remove("_secrets");

        DefaultSecretProvider secretProvider = new DefaultSecretProvider(secretContext, secretMounts, secretStore);

        PrivilegedVariables privilegedVariables = GrantedPrivilegedVariables.build(
                mergedRequest.getLocalConfig().getNestedOrGetEmpty("_env"),
                GrantedPrivilegedVariables.privilegedSecretProvider(secretContext, secretStore));

        OperatorContext context = new DefaultOperatorContext(
                projectPath, mergedRequest, secretProvider, privilegedVariables, limits);

        Operator operator = factory.newOperator(context);

        if (mergedRequest.isCancelRequested()) {
            // NOTE: In the case of failure to perform cleanup,
            // target resources continue to remain
            operator.cleanup(mergedRequest);
            // Throw exception to stop the task
            throw new TaskExecutionException(String.format(ENGLISH,
                    "Got a cancel-requested: attempt_id=%d, task_id=%d",
                    mergedRequest.getAttemptId(), mergedRequest.getTaskId()));
        }

        return operator.run();
    }

    private void heartbeat()
    {
        try {
            Map<Integer, List<TaskRequest>> sites = runningTaskMap.values().stream()
                .collect(Collectors.groupingBy(
                            TaskRequest::getSiteId,
                            Collectors.mapping(Function.identity(), Collectors.toList())
                            ));
            for (Map.Entry<Integer, List<TaskRequest>> pair : sites.entrySet()) {
                int siteId = pair.getKey();
                List<TaskRequest> taskRequests = pair.getValue();
                callback.taskHeartbeat(siteId, taskRequests, agentId, agentConfig.getLockRetentionTime());
            }
        }
        catch (Throwable t) {
            logger.error(
                    LogMarkers.UNEXPECTED_SERVER_ERROR,
                    "Uncaught exception during sending task heartbeats to a server. Ignoring. Heartbeat thread will be retried.", t);
            errorReporter.reportUncaughtError(t);
            metrics.increment(Category.AGENT, "uncaughtErrors");
        }
    }

    public static String formatExceptionMessage(Throwable ex)
    {
        StringBuilder sb = new StringBuilder();
        collectExceptionMessage(sb, ex, new StringBuffer());
        return sb.toString();
    }

    public static void collectExceptionMessage(StringBuilder sb, Throwable ex, StringBuffer used)
    {
        String message = ex.getMessage();
        if (isNullOrEmpty(message)) {
            message = ex.getClass().getSimpleName();
        }
        if (used.indexOf(message) == -1) {
            used.append("\n").append(message);
            if (sb.length() > 0) {
                sb.append("\n> ");
            }
            sb.append(message);
            if (!(ex instanceof TaskExecutionException)) {
                // skip TaskExecutionException because it's expected to have well-formatted message
                sb.append(" (");
                sb.append(ex.getClass().getSimpleName()
                            .replaceFirst("(?:Exception|Error)$", "")
                            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
                            .replaceAll("([a-z])([A-Z])", "$1 $2")
                            .toLowerCase());
                sb.append(")");
            }
        }
        if (ex.getCause() != null) {
            collectExceptionMessage(sb, ex.getCause(), used);
        }
        for (Throwable t : ex.getSuppressed()) {
            collectExceptionMessage(sb, t, used);
        }
    }

    private static final String[] CONFIG_KEYS_FOR_LOGGING = {
            "project_id",
            "session_id",
            "session_time",
            "attempt_id",
            "task_name",
            "last_session_time",
            "last_executed_session_time",
            "next_session_time",
            "session_uuid",
            "timezone"
    };

    @VisibleForTesting
    public Config filterConfigForLogging(Config src)
    {
        Config dst = cf.create();
        for (String key : CONFIG_KEYS_FOR_LOGGING) {
            Optional<Object> v = src.getOptional(key, Object.class);
            if (v.isPresent()) {
                dst.set(key, v.get());
            }
        }
        return dst;
    }
}

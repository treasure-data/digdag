package io.digdag.core.agent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.log.LogLevel;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.TaskLogger;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.ErrorReporter;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretAccessPolicy;
import io.digdag.spi.SecretSelector;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public class OperatorManager
{
    private static Logger logger = LoggerFactory.getLogger(OperatorManager.class);

    protected final AgentConfig agentConfig;
    protected final AgentId agentId;
    protected final TaskCallbackApi callback;
    private final WorkspaceManager workspaceManager;
    private final WorkflowCompiler compiler;
    private final ConfigFactory cf;
    private final ConfigEvalEngine evalEngine;
    private final OperatorRegistry registry;
    private final SecretStoreManager secretStoreManager;
    private final SecretAccessPolicy secretAccessPolicy;

    private final ScheduledExecutorService heartbeatScheduler;
    private final ConcurrentHashMap<Long, TaskRequest> runningTaskMap = new ConcurrentHashMap<>();  // {taskId => TaskRequest}

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    public OperatorManager(AgentConfig agentConfig, AgentId agentId,
            TaskCallbackApi callback, WorkspaceManager workspaceManager,
            WorkflowCompiler compiler, ConfigFactory cf,
            ConfigEvalEngine evalEngine, OperatorRegistry registry,
            SecretStoreManager secretStoreManager, SecretAccessPolicy secretAccessPolicy)
    {
        this.agentConfig = agentConfig;
        this.agentId = agentId;
        this.callback = callback;
        this.workspaceManager = workspaceManager;
        this.compiler = compiler;
        this.cf = cf;
        this.evalEngine = evalEngine;

        this.registry = registry;
        this.secretStoreManager = secretStoreManager;
        this.secretAccessPolicy = secretAccessPolicy;

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

    public void run(TaskRequest request)
    {
        long taskId = request.getTaskId();

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(request.getTaskName())) {
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

    private void runWithHeartbeat(TaskRequest request)
    {
        try {
            workspaceManager.withExtractedArchive(request, () -> callback.openArchive(request), (workspacePath) -> {
                try {
                    runWithWorkspace(workspacePath, request);
                }
                catch (TaskExecutionException ex) {
                    if (ex.getRetryInterval().isPresent()) {
                        if (!ex.getError(cf).isPresent()) {
                            logger.debug("Retrying task {}", ex.toString());
                        }
                        else {
                            logger.error("Task failed, retrying", ex);
                        }
                        callback.retryTask(request.getSiteId(),
                                request.getTaskId(), request.getLockId(), agentId,
                                ex.getRetryInterval().get(), ex.getStateParams(cf).get(),
                                ex.getError(cf));
                    }
                    else {
                        logger.error("Task {} failed.\n{}", request.getTaskName(), formatExceptionMessage(ex));
                        logger.debug("", ex);
                        // TODO use debug to log stacktrace here
                        callback.taskFailed(request.getSiteId(),
                                request.getTaskId(), request.getLockId(), agentId,
                                ex.getError(cf).get());  // TODO is error set?
                    }
                }
                catch (RuntimeException ex) {
                    if (ex instanceof ConfigException) {
                        logger.error("Configuration error at task {}: {}", request.getTaskName(), formatExceptionMessage(ex));
                    }
                    else {
                        logger.error("Task failed with unexpected error: {}", ex.getMessage(), ex);
                    }
                    callback.taskFailed(request.getSiteId(),
                            request.getTaskId(), request.getLockId(), agentId,
                            buildExceptionErrorConfig(ex).toConfig(cf));  // no retry
                }
                return true;
            });
        }
        catch (RuntimeException | IOException ex) {
            // exception happend in workspaceManager
            logger.error("Task failed with unexpected error: {}", ex.getMessage(), ex);
            callback.taskFailed(request.getSiteId(),
                    request.getTaskId(), request.getLockId(), agentId,
                    buildExceptionErrorConfig(ex).toConfig(cf));
        }
    }

    private void runWithWorkspace(Path workspacePath, TaskRequest request)
        throws TaskExecutionException
    {
        // evaluate config and creates the complete merged config.
        Config config;
        try {
            Config all = RuntimeParams.buildRuntimeParams(request.getConfig().getFactory(), request).deepCopy();
            all.merge(request.getConfig());  // export / carry params (TaskRequest.config sent by WorkflowExecutor doesn't include config of this task)
            Config evalParams = all.deepCopy();
            all.merge(request.getLocalConfig());

            config = evalEngine.eval(all, evalParams);
        }
        catch (RuntimeException | TemplateException ex) {
            throw new RuntimeException("Failed to process task config templates", ex);
        }
        logger.debug("evaluated config: {}", config);

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
                callback.taskSucceeded(request.getSiteId(),
                        request.getTaskId(), request.getLockId(), agentId,
                        TaskResult.empty(cf));
                return;
            }
            type = operatorKey.get().substring(0, operatorKey.get().length() - 1);
            Object file = config.get(operatorKey.get(), Object.class);
            config.set("_type", type);
            config.set("_command", file);
            logger.info("{}>: {}", type, file);
            shouldBeUsedKeys.remove(operatorKey.get());
        }

        CheckedConfig checkedConfig = new CheckedConfig(config);

        TaskRequest mergedRequest = TaskRequest.builder()
            .from(request)
            .config(checkedConfig)
            .build();

        // re-get workdir from CheckedConfig
        String workdir = checkedConfig.get("_workdir", String.class, "");

        TaskResult result = callExecutor(workspacePath.resolve(workdir), type, mergedRequest);

        if (!checkedConfig.isAllUsed()) {
            List<String> usedKeys = checkedConfig.getUsedKeys();
            shouldBeUsedKeys.removeAll(usedKeys);
            if (!shouldBeUsedKeys.isEmpty()) {
                warnUnusedKeys(request, shouldBeUsedKeys, usedKeys);
            }
        }

        callback.taskSucceeded(request.getSiteId(),
                request.getTaskId(), request.getLockId(), agentId,
                result);
    }

    private void warnUnusedKeys(TaskRequest request, Set<String> shouldBeUsedButNotUsedKeys, List<String> candidateKeys)
    {
        for (String key : shouldBeUsedButNotUsedKeys) {
            logger.error("Parameter '{}' is not used at task {}.", key, request.getTaskName());
            List<String> suggestions = EditDistance.suggest(key, candidateKeys, 0.50);
            if (!suggestions.isEmpty()) {
                logger.error("  > Did you mean {}?", suggestions);
            }
        }
    }

    protected TaskResult callExecutor(Path workspacePath, String type, TaskRequest mergedRequest)
    {
        OperatorFactory factory = registry.get(mergedRequest, type);
        if (factory == null) {
            throw new ConfigException("Unknown task type: " + type);
        }

        Operator operator = factory.newTaskExecutor(workspacePath, mergedRequest);

        SecretStore secretStore = secretStoreManager.getSecretStore(mergedRequest.getSiteId());

        Config grants = mergedRequest.getConfig().getNestedOrGetEmpty("_secrets");

        SecretFilter operatorSecretFilter = SecretFilter.of(
                operator.secretSelectors().stream().map(SecretSelector::of).collect(Collectors.toList()));

        SecretAccessContext secretContext = SecretAccessContext.builder()
                .siteId(mergedRequest.getSiteId())
                .projectId(mergedRequest.getProjectId())
                .revision(mergedRequest.getRevision().get())
                .workflowName(mergedRequest.getWorkflowName())
                .taskName(mergedRequest.getTaskName())
                .operatorType(type)
                .build();

        DefaultSecretProvider secretProvider = new DefaultSecretProvider(
                secretContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        TaskExecutionContext taskExecutionContext = new DefaultTaskExecutionContext(secretProvider);

        return operator.run(taskExecutionContext);
    }

    private void heartbeat()
    {
        try {
            Map<Integer, List<String>> sites = runningTaskMap.values().stream()
                .collect(Collectors.groupingBy(
                            TaskRequest::getSiteId,
                            Collectors.mapping(TaskRequest::getLockId, Collectors.toList())
                            ));
            for (Map.Entry<Integer, List<String>> pair : sites.entrySet()) {
                int siteId = pair.getKey();
                List<String> lockIds = pair.getValue();
                callback.taskHeartbeat(siteId, lockIds, agentId, agentConfig.getLockRetentionTime());
            }
        }
        catch (Throwable t) {
            logger.error("Uncaught exception during sending task heartbeats to a server. Ignoring. Heartbeat thread will be retried.", t);
            errorReporter.reportUncaughtError(t);
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
            sb.append(" (");
            sb.append(ex.getClass().getSimpleName()
                        .replaceFirst("(?:Exception|Error)$", "")
                        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
                        .replaceAll("([a-z])([A-Z])", "$1 $2")
                        .toLowerCase());
            sb.append(")");
        }
        if (ex.getCause() != null) {
            collectExceptionMessage(sb, ex.getCause(), used);
        }
        for (Throwable t : ex.getSuppressed()) {
            collectExceptionMessage(sb, t, used);
        }
    }
}

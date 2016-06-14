package io.digdag.core.agent;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.log.TaskLogger;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.LogLevel;
import io.digdag.spi.*;
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
    private final Map<String, OperatorFactory> executorTypes;

    private final ScheduledExecutorService heartbeatScheduler;
    private final ConcurrentHashMap<Long, TaskRequest> runningTaskMap = new ConcurrentHashMap<>();  // {taskId => TaskRequest}

    @Inject
    public OperatorManager(AgentConfig agentConfig, AgentId agentId,
            TaskCallbackApi callback, WorkspaceManager workspaceManager,
            WorkflowCompiler compiler, ConfigFactory cf,
            ConfigEvalEngine evalEngine, Set<OperatorFactory> factories)
    {
        this.agentConfig = agentConfig;
        this.agentId = agentId;
        this.callback = callback;
        this.workspaceManager = workspaceManager;
        this.compiler = compiler;
        this.cf = cf;
        this.evalEngine = evalEngine;

        ImmutableMap.Builder<String, OperatorFactory> builder = ImmutableMap.builder();
        for (OperatorFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.executorTypes = builder.build();

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
            workspaceManager.withExtractedArchive(request, (workspacePath) -> {
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

            // workdir can't include ${...}.
            // TODO throw exeption if workdir includes ${...}.
            String workdir = all.get("_workdir", String.class, "");

            config = evalEngine.eval(workspacePath.resolve(workdir), all, evalParams);
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
        OperatorFactory factory = executorTypes.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown task type: " + type);
        }
        Operator executor = factory.newTaskExecutor(workspacePath, mergedRequest);

        return executor.run();
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
            logger.error("An uncaught exception is ignored. Heartbeat thread will be retried.", t);
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
        if (ex.getMessage() != null && used.indexOf(ex.getMessage()) == -1) {
            used.append("\n").append(ex.getMessage());
            if (sb.length() > 0) {
                sb.append("\n> ");
            }
            sb.append(ex.getMessage());
        }
        if (ex.getCause() != null) {
            collectExceptionMessage(sb, ex.getCause(), used);
        }
        for (Throwable t : ex.getSuppressed()) {
            collectExceptionMessage(sb, t, used);
        }
    }
}

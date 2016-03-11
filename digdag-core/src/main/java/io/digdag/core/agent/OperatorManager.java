package io.digdag.core.agent;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Arrays;
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
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.LogLevel;
import io.digdag.spi.*;

public class OperatorManager
{
    private static Logger logger = LoggerFactory.getLogger(OperatorManager.class);

    protected final AgentConfig config;
    protected final AgentId agentId;
    protected final TaskCallbackApi callback;
    private final ArchiveManager archiveManager;
    private final ConfigLoaderManager configLoader;
    private final WorkflowCompiler compiler;
    private final ConfigFactory cf;
    private final ConfigEvalEngine evalEngine;
    private final Map<String, OperatorFactory> executorTypes;

    private final ScheduledExecutorService heartbeatScheduler;
    private final ConcurrentHashMap<Long, String> lockIdMap = new ConcurrentHashMap<>();

    @Inject
    public OperatorManager(AgentConfig config, AgentId agentId,
            TaskCallbackApi callback, ArchiveManager archiveManager,
            ConfigLoaderManager configLoader, WorkflowCompiler compiler, ConfigFactory cf,
            ConfigEvalEngine evalEngine, Set<OperatorFactory> factories)
    {
        this.config = config;
        this.agentId = agentId;
        this.callback = callback;
        this.archiveManager = archiveManager;
        this.configLoader = configLoader;
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
                config.getHeartbeatInterval(), config.getHeartbeatInterval(),
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
        // nextState is mutable
        Config nextState = request.getLastStateParams();

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(request.getTaskName())) {
            try {
                runWithHeartbeat(request, nextState);
            }
            catch (RuntimeException | IOException ex) {
                logger.error("Task failed", ex);
                Config error = makeExceptionError(cf, ex);
                callback.taskFailed(
                        request.getTaskId(), request.getLockId(), agentId,
                        error);  // no retry
            }
        }
    }

    private void runWithHeartbeat(TaskRequest request, Config nextState)
        throws IOException
    {
        long taskId = request.getTaskId();

        lockIdMap.put(taskId, request.getLockId());
        try {
            TaskContextLogging.enter(LogLevel.DEBUG, callback.newTaskLogger(request));
            try {
                archiveManager.withExtractedArchive(request, (archivePath) -> {
                    runWithArchive(archivePath, request, nextState);
                    return true;
                });
            }
            finally {
                TaskContextLogging.leave();
            }
        }
        finally {
            lockIdMap.remove(taskId);
        }
    }

    private void runWithArchive(Path archivePath, TaskRequest request, Config nextState)
    {
        long taskId = request.getTaskId();

        try {
            // evaluate config and creates the complete merged config.
            Config config;
            try {
                Config all = RuntimeParams.buildRuntimeParams(request.getConfig().getFactory(), request).deepCopy();
                all.merge(request.getConfig());  // export / carry params (TaskRequest.config sent by WorkflowExecutor doesn't include config of this task)
                Config evalParams = all.deepCopy();
                all.merge(request.getLocalConfig());
                config = evalEngine.eval(archivePath, all, evalParams);
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
                    callback.taskSucceeded(
                            taskId, request.getLockId(), agentId,
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

            CheckedConfig checkedConfig = new CheckedConfig(config, shouldBeUsedKeys);

            TaskRequest mergedRequest = TaskRequest.builder()
                .from(request)
                .config(checkedConfig)
                .build();

            TaskResult result = callExecutor(archivePath, type, mergedRequest);

            warnUnusedKeys(checkedConfig.getUnusedKeys(), request);

            callback.taskSucceeded(
                    taskId, request.getLockId(), agentId,
                    result);
        }
        catch (TaskExecutionException ex) {
            if (ex.getRetryInterval().isPresent()) {
                if (ex.getError().isPresent()) {
                    logger.debug("Retrying task {}", ex.toString());
                }
                else {
                    logger.error("Task failed, retrying", ex);
                }
                callback.retryTask(
                        taskId, request.getLockId(), agentId,
                        ex.getRetryInterval().get(), ex.getStateParams().get(),
                        ex.getError());
            }
            else {
                logger.error("Task failed", ex);
                callback.taskFailed(
                        taskId, request.getLockId(), agentId,
                        ex.getError().get());  // TODO is error set?
            }
        }
    }

    private void warnUnusedKeys(List<String> unusedKeys, TaskRequest request)
    {
        if (!unusedKeys.isEmpty()) {
            logger.warn("Some parameters are not used at {}: {}", request.getTaskName(), unusedKeys);
        }
    }

    protected TaskResult callExecutor(Path archivePath, String type, TaskRequest mergedRequest)
    {
        OperatorFactory factory = executorTypes.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown task type: " + type);
        }
        Operator executor = factory.newTaskExecutor(archivePath, mergedRequest);

        return executor.run();
    }

    public static Config makeExceptionError(ConfigFactory cf, Exception ex)
    {
        return cf.create()
            .set("message", ex.toString())
            .set("stacktrace",
                    Arrays.asList(ex.getStackTrace())
                    .stream()
                    .map(it -> it.toString())
                    .collect(Collectors.joining(", ")));
    }

    private void heartbeat()
    {
        try {
            List<String> lockedIds = ImmutableList.copyOf(lockIdMap.values());
            if (!lockedIds.isEmpty()) {
                callback.taskHeartbeat(lockedIds, agentId, config.getLockRetentionTime());
            }
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Heartbeat thread will be retried.", t);
        }
    }
}

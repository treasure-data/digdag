package io.digdag.core.agent;

import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.SubtaskMatchPattern;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.spi.*;

public class TaskRunnerManager
{
    private static Logger logger = LoggerFactory.getLogger(TaskRunnerManager.class);

    private final TaskCallbackApi callback;
    private final ArchiveManager archiveManager;
    private final ConfigLoaderManager configLoader;
    private final WorkflowCompiler compiler;
    private final ConfigFactory cf;
    private final ConfigEvalEngine evalEngine;
    private final Map<String, TaskRunnerFactory> executorTypes;

    @Inject
    public TaskRunnerManager(TaskCallbackApi callback, ArchiveManager archiveManager,
            ConfigLoaderManager configLoader, WorkflowCompiler compiler, ConfigFactory cf,
            ConfigEvalEngine evalEngine, Set<TaskRunnerFactory> factories)
    {
        this.callback = callback;
        this.archiveManager = archiveManager;
        this.configLoader = configLoader;
        this.compiler = compiler;
        this.cf = cf;
        this.evalEngine = evalEngine;

        ImmutableMap.Builder<String, TaskRunnerFactory> builder = ImmutableMap.builder();
        for (TaskRunnerFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.executorTypes = builder.build();
    }

    public void run(String agentId, TaskRequest request)
    {
        // nextState is mutable
        Config nextState = request.getLastStateParams();

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(request.getTaskName())) {
            archiveManager.withExtractedArchive(request, (archivePath) -> {
                runWithArchive(agentId, archivePath, request, nextState);
                return true;
            });
        }
        catch (RuntimeException | IOException ex) {
            logger.error("Task failed", ex);
            Config error = makeExceptionError(cf, ex);
            callback.taskFailed(
                    request.getTaskId(), request.getLockId(), agentId,
                    error, nextState,
                    Optional.absent());  // no retry
        }
    }

    private void runWithArchive(String agentId, Path archivePath, TaskRequest request, Config nextState)
    {
        long taskId = request.getTaskId();

        try {
            // TaskRequest.config sent by WorkflowExecutor doesn't include local config of this task.
            // here evaluates local config and creates the complete merged config.
            Config config = request.getConfig().deepCopy();
            try {
                Config evaluatedLocalConfig = evalEngine.eval(archivePath, request.getLocalConfig(), request.getConfig());
                config.setAll(evaluatedLocalConfig);
            }
            catch (RuntimeException | TemplateException ex) {
                throw new RuntimeException("Failed to rebuild task config", ex);
            }
            logger.debug("evaluated config: {}", config);

            TaskRequest mergedRequest = TaskRequest.builder()
                .from(request)
                .config(config)
                .build();

            String type;
            if (config.has("type")) {
                type = config.get("type", String.class);
                logger.info("type: {}", type);
            }
            else {
                java.util.Optional<String> commandKey = config.getKeys()
                    .stream()
                    .filter(key -> key.endsWith(">"))
                    .findFirst();
                if (!commandKey.isPresent()) {
                    // TODO warning
                    callback.taskSucceeded(
                            taskId, request.getLockId(), agentId,
                            nextState, cf.create(),
                            TaskReport.empty(cf));
                    return;
                }
                type = commandKey.get().substring(0, commandKey.get().length() - 1);
                Object command = config.get(commandKey.get(), Object.class);
                config.set("type", type);
                config.set("command", command);
                logger.info("{}>: {}", type, command);
            }

            TaskRunnerFactory factory = executorTypes.get(type);
            if (factory == null) {
                throw new ConfigException("Unknown task type: " + type);
            }
            TaskRunner executor = factory.newTaskExecutor(archivePath, mergedRequest);

            TaskResult result;
            try {
                result = executor.run();
            }
            finally {
                nextState = executor.getStateParams();
            }

            callback.taskSucceeded(
                    taskId, request.getLockId(), agentId,
                    nextState, result.getSubtaskConfig(),
                    result.getReport());
        }
        catch (TaskExecutionException ex) {
            if (ex.getError().isPresent()) {
                logger.error("Task failed", ex);
                callback.taskFailed(
                        taskId, request.getLockId(), agentId,
                        ex.getError().get(), nextState,
                        ex.getRetryInterval());
            }
            else {
                logger.debug("Retrying task {}", ex.toString());
                callback.taskPollNext(
                        taskId, request.getLockId(), agentId,
                        nextState, ex.getRetryInterval().get());
            }
        }
    }

    public static Config makeExceptionError(ConfigFactory cf, Exception ex)
    {
        return cf.create()
            .set("error", ex.toString())
            .set("stacktrace",
                    Arrays.asList(ex.getStackTrace())
                    .stream()
                    .map(it -> it.toString())
                    .collect(Collectors.joining(", ")));
    }
}

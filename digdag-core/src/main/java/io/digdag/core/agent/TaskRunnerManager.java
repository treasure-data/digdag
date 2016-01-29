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

    public void run(TaskRequest request)
    {
        // nextState is mutable
        Config nextState = request.getLastStateParams();

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(request.getTaskInfo().getFullName())) {
            archiveManager.withExtractedArchive(request, (archivePath) -> {
                runWithArchive(archivePath, request, nextState);
                return true;
            });
        }
        catch (RuntimeException | IOException ex) {
            logger.error("Task failed", ex);
            Config error = makeExceptionError(cf, ex);
            callback.taskFailed(request.getTaskInfo().getId(),
                    error, nextState,
                    Optional.absent());  // no retry
        }
    }

    private void runWithArchive(Path archivePath, TaskRequest request, Config nextState)
    {
        long taskId = request.getTaskInfo().getId();

        try {
            // TaskRequest.config sent by WorkflowExecutor doesn't include local config of this task.
            // here reloads local config and creates the complete merged config.
            Config config;
            try {
                // TODO here is a known bug:
                //      if `digdag run` runs with a subtask option that points a nested task,
                //      taskFullName doesn't match with reloaded config. So it throws following
                //      RuntimeException.
                config = evalTaskConfig(archivePath, request.getDagfilePath(),
                        request.getWorkflowName(), request.getTaskInfo().getFullName(),
                        request.getConfig(), request.getLocalConfig());
            }
            catch (IOException | RuntimeException | ConfigEvalException ex) {
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
                    callback.taskSucceeded(taskId,
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

            callback.taskSucceeded(taskId,
                    nextState, result.getSubtaskConfig(),
                    result.getReport());
        }
        catch (TaskExecutionException ex) {
            if (ex.getError().isPresent()) {
                logger.error("Task failed", ex);
                callback.taskFailed(taskId,
                        ex.getError().get(), nextState,
                        ex.getRetryInterval());
            }
            else {
                logger.debug("Retrying task {}", ex.toString());
                callback.taskPollNext(
                        taskId,
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

    private Config evalTaskConfig(Path archivePath, Optional<String> dagfilePath,
            String workflowName, String taskFullName,
            Config params, Config localConfig)
        throws IOException, ConfigEvalException
    {
        System.out.println("Reevaluating config using params: "+params);

        if (dagfilePath.isPresent()) {
            Dagfile dagfile = configLoader.loadParameterizedFile(
                    archivePath.resolve(dagfilePath.get()).toFile(),
                    params).convert(Dagfile.class);
            WorkflowDefinition def = findDefinition(dagfile, workflowName);
            Workflow workflow = compiler.compile(def.getName(), def.getConfig());
            WorkflowTask task = findTask(workflow, taskFullName);
            localConfig = task.getConfig();

            System.out.println("Reevaluated local config: "+localConfig);

        }

        Config config =
            localConfig.deepCopy()
            .setAll(params);

        return evalEngine.eval(archivePath, config);
    }

    private WorkflowDefinition findDefinition(Dagfile dagfile, String workflowName)
    {
        for (WorkflowDefinition def : dagfile.getWorkflowList().get()) {
            if (def.getName().equals(workflowName)) {
                return def;
            }
        }
        throw new RuntimeException("Workflow doesn't exist in the reloaded workflow");
    }

    private WorkflowTask findTask(Workflow workflow, String taskFullName)
    {
        try {
            int index = SubtaskMatchPattern.compile(taskFullName).findIndex(workflow.getTasks());
            return workflow.getTasks().get(index);
        }
        catch (TaskMatchPattern.MultipleTaskMatchException | TaskMatchPattern.NoMatchException ex) {
        throw new RuntimeException("Task doesn't exist in the reloaded workflow");
        }
    }
}

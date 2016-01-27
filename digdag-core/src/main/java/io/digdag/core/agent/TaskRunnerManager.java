package io.digdag.core.agent;

import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.*;

public class TaskRunnerManager
{
    private static Logger logger = LoggerFactory.getLogger(TaskRunnerManager.class);

    private final TaskCallbackApi callback;
    private final ConfigFactory cf;
    private final ConfigEvalEngine evalEngine;
    private final Map<String, TaskRunnerFactory> executorTypes;

    @Inject
    public TaskRunnerManager(TaskCallbackApi callback, ConfigFactory cf,
            ConfigEvalEngine evalEngine, Set<TaskRunnerFactory> factories)
    {
        this.callback = callback;
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
        long taskId = request.getTaskInfo().getId();
        Config config = request.getConfig().deepCopy();
        Config nextState = request.getLastStateParams();

        // set task name to thread name so that logger shows it
        try (SetThreadName threadName = new SetThreadName(request.getTaskInfo().getFullName())) {

            config = evalEngine.eval(config);
            logger.trace("evaluated config: {}", config);

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
            TaskRunner executor = factory.newTaskExecutor(
                    TaskRequest.builder()
                        .from(request)
                        .config(config)
                        .build());

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
        catch (Exception ex) {
            logger.error("Task failed", ex);
            Config error = makeExceptionError(cf, ex);
            callback.taskFailed(taskId,
                    error, nextState,
                    Optional.absent());  // no retry
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

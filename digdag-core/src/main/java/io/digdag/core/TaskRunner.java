package io.digdag.core;

import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;

public class TaskRunner
{
    private final TaskApi api;
    private final ConfigSourceFactory cf;
    private final Map<String, TaskExecutorFactory> executorTypes;

    @Inject
    public TaskRunner(TaskApi api, ConfigSourceFactory cf, Set<TaskExecutorFactory> factories)
    {
        this.api = api;
        this.cf = cf;

        ImmutableMap.Builder<String, TaskExecutorFactory> builder = ImmutableMap.builder();
        for (TaskExecutorFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.executorTypes = builder.build();
    }

    public void run(Action action)
    {
        ConfigSource config = action.getConfig();
        ConfigSource params = action.getParams();
        ConfigSource state = action.getStateParams();

        try {
            if (!config.has("type")) {
                java.util.Optional<String> commandKey = config.getKeys()
                    .stream()
                    .filter(key -> key.endsWith(">"))
                    .findFirst();
                if (!commandKey.isPresent()) {
                    // TODO warning
                    api.taskSucceeded(action.getTaskId(),
                            state, cf.create(),
                            cf.create(), TaskReport.empty(cf));
                    return;
                }
                config.set("type", commandKey.get().substring(0, commandKey.get().length() - 1));
                config.set("command", config.get(commandKey.get(), Object.class));
            }
            String type = config.get("type", String.class);

            TaskExecutorFactory factory = executorTypes.get(type);
            if (factory == null) {
                throw new ConfigException("Unknown task type: "+type);
            }
            TaskExecutor executor = factory.newTaskExecutor(config, params, state);

            TaskResult result;
            try {
                result = executor.run();
            }
            finally {
                state = executor.getState();
            }

            api.taskSucceeded(action.getTaskId(),
                    state, result.getSubtaskConfig(),
                    result.getCarryParams(), result.getReport());
        }
        catch (TaskExecutionException ex) {
            if (ex.getError().isPresent()) {
                api.taskFailed(action.getTaskId(),
                        ex.getError().get(), state,
                        ex.getRetryInterval());
            }
            else {
                api.taskPollNext(action.getTaskId(),
                        state, ex.getRetryInterval().get());
            }
        }
        catch (Exception ex) {
            ConfigSource error = makeExceptionError(cf, ex);
            api.taskFailed(action.getTaskId(),
                    error, state,
                    Optional.absent());  // no retry
        }
    }

    public static ConfigSource makeExceptionError(ConfigSourceFactory cf, Exception ex)
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

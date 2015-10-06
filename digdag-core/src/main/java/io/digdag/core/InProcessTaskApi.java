package io.digdag.core;

import com.google.inject.Inject;
import com.google.common.base.*;

public class InProcessTaskApi
        implements TaskApi
{
    private final SessionExecutor exec;

    @Inject
    public InProcessTaskApi(SessionExecutor exec)
    {
        this.exec = exec;
    }

    @Override
    public void taskFinished(long taskId,
            ConfigSource stateParams,
            ConfigSource subtaskConfig,
            Optional<ConfigSource> error,
            Optional<Integer> retryInterval,
            Optional<ConfigSource> carryParams,
            Optional<TaskReport> report)
    {
        exec.taskFinished(taskId, stateParams, subtaskConfig, error, retryInterval, carryParams, report);
    }
}

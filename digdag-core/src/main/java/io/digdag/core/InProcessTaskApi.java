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
    public void taskSucceeded(long taskId,
            ConfigSource stateParams, ConfigSource subtaskConfig,
            TaskReport report)
    {
        exec.taskSucceeded(taskId, stateParams, subtaskConfig, report);
    }

    @Override
    public void taskFailed(long taskId,
            ConfigSource error, ConfigSource stateParams,
            Optional<Integer> retryInterval)
    {
        exec.taskFailed(taskId, error, stateParams, retryInterval);
    }

    @Override
    public void taskPollNext(long taskId,
            ConfigSource stateParams, int retryInterval)
    {
        exec.taskPollNext(taskId, stateParams, retryInterval);
    }
}

package io.digdag.core;

import com.google.inject.Inject;
import com.google.common.base.*;
import io.digdag.core.config.Config;

public class InProcessTaskApi
        implements TaskApi
{
    private final WorkflowExecutor exec;

    @Inject
    public InProcessTaskApi(WorkflowExecutor exec)
    {
        this.exec = exec;
    }

    @Override
    public void taskSucceeded(long taskId,
            Config stateParams, Config subtaskConfig,
            TaskReport report)
    {
        exec.taskSucceeded(taskId, stateParams, subtaskConfig, report);
    }

    @Override
    public void taskFailed(long taskId,
            Config error, Config stateParams,
            Optional<Integer> retryInterval)
    {
        exec.taskFailed(taskId, error, stateParams, retryInterval);
    }

    @Override
    public void taskPollNext(long taskId,
            Config stateParams, int retryInterval)
    {
        exec.taskPollNext(taskId, stateParams, retryInterval);
    }
}

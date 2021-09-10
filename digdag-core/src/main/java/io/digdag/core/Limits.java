package io.digdag.core;

import io.digdag.client.config.Config;

import javax.inject.Inject;

public class Limits
{
    private static final long MAX_WORKFLOW_TASKS = Long.parseLong(
            System.getProperty("io.digdag.limits.maxWorkflowTasks", "1000"));

    private static final long MAX_ATTEMPTS = Long.parseLong(
            System.getProperty("io.digdag.limits.maxAttempts", "100"));

    private final long numOfMaxWorkflowTasks;
    private final long numOfMaxAttempts;

    @Inject
    public Limits(Config systemConfig)
    {
        this.numOfMaxWorkflowTasks =  systemConfig.get("executor.task_max_run", long.class, MAX_WORKFLOW_TASKS);
        this.numOfMaxAttempts = systemConfig.get("executor.attempt_max_run", long.class, MAX_ATTEMPTS);
    }

    public long maxWorkflowTasks()
    {
        return numOfMaxWorkflowTasks;
    }

    public long maxAttempts()
    {
        return numOfMaxAttempts;
    }
}

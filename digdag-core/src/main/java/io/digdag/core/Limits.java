package io.digdag.core;

public class Limits
{
    private static final long MAX_WORKFLOW_TASKS = Long.valueOf(
            System.getProperty("io.digdag.limits.maxWorkflowTasks", "1000"));

    private static final long MAX_ATTEMPTS = Long.valueOf(
            System.getProperty("io.digdag.limits.maxAttempts", "100"));

    // TODO (dano): this should be configurable by config file etc and not just system property

    public static long maxWorkflowTasks()
    {
        return MAX_WORKFLOW_TASKS;
    }

    public static long maxAttempts()
    {
        return MAX_ATTEMPTS;
    }
}

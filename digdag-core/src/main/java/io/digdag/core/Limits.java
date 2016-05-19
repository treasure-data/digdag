package io.digdag.core;

import io.digdag.core.workflow.TaskControl;

public class Limits
{
    private static final long MAX_WORKFLOW_TASKS = Long.valueOf(
            System.getProperty(TaskControl.class.getName() + "io.digdag.limits.maxWorkflowTasks", "1000"));

    // TODO (dano): this should be configurable by config file etc and not just system property

    public static long maxWorkflowTasks() {
        return MAX_WORKFLOW_TASKS;
    }
}

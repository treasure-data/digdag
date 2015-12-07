package io.digdag.core.workflow;

import io.digdag.core.session.TaskStateCode;

public final class Tasks
{
    private Tasks() { }

    public static boolean isDone(TaskStateCode code)
    {
        for (TaskStateCode ds : TaskStateCode.doneStates()) {
            if (code == ds) {
                return true;
            }
        }
        return false;
    }
}

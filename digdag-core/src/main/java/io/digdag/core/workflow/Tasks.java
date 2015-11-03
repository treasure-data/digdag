package io.digdag.core.workflow;

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

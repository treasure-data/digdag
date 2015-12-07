package io.digdag.core.workflow;

import java.util.List;
import java.util.AbstractList;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.annotation.JsonCreator;

public class WorkflowTaskList
        extends AbstractList<WorkflowTask>
{
    private final List<WorkflowTask> tasks;

    private WorkflowTaskList(List<WorkflowTask> tasks)
    {
        this.tasks = ImmutableList.copyOf(tasks);
    }

    @JsonCreator
    public static WorkflowTaskList of(List<WorkflowTask> tasks)
    {
        return new WorkflowTaskList(tasks);
    }

    @Override
    public WorkflowTask get(int index)
    {
        return tasks.get(index);
    }

    @Override
    public int size()
    {
        return tasks.size();
    }

    @Override
    public boolean equals(Object another)
    {
        if (!(another instanceof WorkflowTaskList)) {
            return false;
        }
        return this.tasks.equals(((WorkflowTaskList) another).tasks);
    }

    @Override
    public int hashCode()
    {
        return tasks.hashCode();
    }
}

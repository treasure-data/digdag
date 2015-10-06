package io.digdag.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class TaskType
{
    public static class Builder
    {
        private int flags = 0;

        public Builder groupingOnly(boolean v)
        {
            if (v) {
                flags |= GROUPING_ONLY;
            }
            else {
                flags &= ~GROUPING_ONLY;
            }
            return this;
        }

        public TaskType build()
        {
            return new TaskType(flags);
        }
    }

    public static final int GROUPING_ONLY = 1;

    @JsonCreator
    public static TaskType of(int flags)
    {
        // TODO validation
        return new TaskType(flags);
    }

    private final int flags;

    public TaskType(int flags)
    {
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public boolean isGroupingOnly()
    {
        return (flags & GROUPING_ONLY) != 0;
    }

    @Override
    public int hashCode()
    {
        return flags;
    }

    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof TaskType && ((TaskType) another).flags == flags);
    }

    @Override
    public String toString()
    {
        // TODO pretty print
        return "TaskType{"+flags+"}";
    }
}

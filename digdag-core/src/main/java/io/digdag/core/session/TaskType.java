package io.digdag.core.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import static com.google.common.base.Preconditions.checkArgument;

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
        return new TaskType(flags);
    }

    private final int flags;

    public TaskType(int flags)
    {
        checkArgument(flags >= 0 && flags < Short.MAX_VALUE, "TaskType must be positive 16-bit signed integer");
        checkArgument((flags & ~GROUPING_ONLY) == 0, "Unknown TaskType is set");
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
        if ((flags & GROUPING_ONLY) != 0) {
            return "TaskType{GROUPING_ONLY}";
        }
        else {
            return "TaskType{}";
        }
    }
}

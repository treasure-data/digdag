package io.digdag.core.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import static com.google.common.base.Preconditions.checkArgument;

public class TaskStateFlags
{
    public static class Builder
    {
        private int flags = 0;

        public Builder groupingOnly(boolean v)
        {
            if (v) {
                flags |= CANCEL_REQUESTED;
            }
            else {
                flags &= ~CANCEL_REQUESTED;
            }
            return this;
        }

        public TaskStateFlags build()
        {
            return new TaskStateFlags(flags);
        }
    }

    public static final int CANCEL_REQUESTED = 1;

    @JsonCreator
    public static TaskStateFlags of(int flags)
    {
        return new TaskStateFlags(flags);
    }

    private final int flags;

    public TaskStateFlags(int flags)
    {
        checkArgument(flags >= 0 && flags < Short.MAX_VALUE, "TaskStateFlags must be positive 16-bit signed integer");
        checkArgument((flags & ~CANCEL_REQUESTED) == 0, "Unknown TaskStateFlags is set");
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public boolean isCancelRequested()
    {
        return (flags & CANCEL_REQUESTED) != 0;
    }

    @Override
    public int hashCode()
    {
        return flags;
    }

    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof TaskStateFlags && ((TaskStateFlags) another).flags == flags);
    }

    @Override
    public String toString()
    {
        if ((flags & CANCEL_REQUESTED) != 0) {
            return "TaskStateFlags{CANCEL_REQUESTED}";
        }
        else {
            return "TaskStateFlags{"+flags+"}";
        }
    }
}

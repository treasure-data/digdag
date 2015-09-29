package io.digdag.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class TaskFlags
{
    public static class Builder
    {
        private int flags = 0;

        public Builder ignoreParentError(boolean v)
        {
            if (v) {
                flags |= IGNORE_PARENT_ERROR;
            }
            else {
                flags &= ~IGNORE_PARENT_ERROR;
            }
            return this;
        }
    }

    public static final int IGNORE_PARENT_ERROR = 1;

    public static TaskFlags of(int flags)
    {
        return new TaskFlags(flags);
    }

    private final int flags;

    /**
     * don't call directly
     */
    @Deprecated
    @JsonCreator
    public TaskFlags(int flags)
    {
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public boolean isIgnoreParentError()
    {
        return (flags & IGNORE_PARENT_ERROR) != 0;
    }

    @Override
    public int hashCode()
    {
        return flags;
    }

    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof TaskFlags && ((TaskFlags) another).flags == flags);
    }

    @Override
    public String toString()
    {
        // TODO pretty print
        return "TaskFlags{"+flags+"}";
    }
}

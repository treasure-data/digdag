package io.digdag.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class TaskErrorMode
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

        public TaskErrorMode build()
        {
            return new TaskErrorMode(flags);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final int IGNORE_PARENT_ERROR = 1;

    public static TaskErrorMode of(int flags)
    {
        return new TaskErrorMode(flags);
    }

    private final int flags;

    /**
     * don't call directly
     */
    @Deprecated
    @JsonCreator
    public TaskErrorMode(int flags)
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
            (another instanceof TaskErrorMode && ((TaskErrorMode) another).flags == flags);
    }

    @Override
    public String toString()
    {
        // TODO pretty print
        return "TaskErrorMode{"+flags+"}";
    }
}

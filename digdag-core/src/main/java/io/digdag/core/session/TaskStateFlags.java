package io.digdag.core.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import static com.google.common.base.Preconditions.checkArgument;

public class TaskStateFlags
{
    public static final int CANCEL_REQUESTED = 1 << 0;
    public static final int DELAYED_ERROR = 1 << 1;
    public static final int DELAYED_GROUP_ERROR = 1 << 2;
    public static final int INITIAL_TASK = 1 << 3;

    @JsonCreator
    public static TaskStateFlags of(int flags)
    {
        return new TaskStateFlags(flags);
    }

    public static TaskStateFlags empty()
    {
        return of(0);
    }

    private final int flags;

    public TaskStateFlags(int flags)
    {
        checkArgument(flags >= 0 && flags < Short.MAX_VALUE, "TaskStateFlags must be positive 16-bit signed integer");
        int unknown = flags
            & ~CANCEL_REQUESTED
            & ~DELAYED_ERROR
            & ~DELAYED_GROUP_ERROR
            & ~INITIAL_TASK;
        checkArgument(unknown == 0, "Unknown TaskStateFlags is set");
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public TaskStateFlags withCancelRequested()
    {
        return TaskStateFlags.of(flags | CANCEL_REQUESTED);
    }

    public boolean isCancelRequested()
    {
        return (flags & CANCEL_REQUESTED) != 0;
    }

    public TaskStateFlags withDelayedError()
    {
        return TaskStateFlags.of(flags | DELAYED_ERROR);
    }

    public boolean isDelayedError()
    {
        return (flags & DELAYED_ERROR) != 0;
    }

    public TaskStateFlags withDelayedGroupError()
    {
        return TaskStateFlags.of(flags | DELAYED_GROUP_ERROR);
    }

    public boolean isDelayedGroupError()
    {
        return (flags & DELAYED_GROUP_ERROR) != 0;
    }

    public TaskStateFlags withInitialTask()
    {
        return TaskStateFlags.of(flags | INITIAL_TASK);
    }

    public boolean isInitialTask()
    {
        return (flags & INITIAL_TASK) != 0;
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
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        if (isCancelRequested()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("CANCEL_REQUESTED");
        }
        else if (isDelayedError()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("DELAYED_ERROR");
        }
        else if (isDelayedGroupError()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("DELAYED_GROUP_ERROR");
        }
        else if (isInitialTask()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("INITIAL_TASK");
        }
        return "TaskStateFlags{"+sb.toString()+"}";
    }
}

package io.digdag.core.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import static com.google.common.base.Preconditions.checkArgument;

public class TaskStateFlags
{
    public static final int CANCEL_REQUESTED = 1;
    public static final int DELAYED_ERROR = 2;
    public static final int DELAYED_GROUP_ERROR = 4;

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
            & ~DELAYED_GROUP_ERROR
            & ~DELAYED_GROUP_ERROR;
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
        return "TaskStateFlags{"+sb.toString()+"}";
    }
}

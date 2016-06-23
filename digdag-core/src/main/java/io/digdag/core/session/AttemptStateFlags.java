package io.digdag.core.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import static com.google.common.base.Preconditions.checkArgument;

public class AttemptStateFlags
{
    public static final int CANCEL_REQUESTED_CODE = 1;
    public static final int DONE_CODE = 2;
    public static final int SUCCESS_CODE = 4;

    @JsonCreator
    public static AttemptStateFlags of(int flags)
    {
        return new AttemptStateFlags(flags);
    }

    public static AttemptStateFlags empty()
    {
        return of(0);
    }

    private final int flags;

    private AttemptStateFlags(int flags)
    {
        checkArgument(flags >= 0 && flags < Short.MAX_VALUE, "AttemptStateFlags must be positive 16-bit signed integer");
        int unknown = flags
            & ~CANCEL_REQUESTED_CODE
            & ~DONE_CODE
            & ~SUCCESS_CODE;
        checkArgument(unknown == 0, "Unknown AttemptStateFlags is set");
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public AttemptStateFlags withCancelRequested()
    {
        return AttemptStateFlags.of(flags | CANCEL_REQUESTED_CODE);
    }

    public boolean isCancelRequested()
    {
        return (flags & CANCEL_REQUESTED_CODE) != 0;
    }

    public AttemptStateFlags withDone()
    {
        return AttemptStateFlags.of(flags | DONE_CODE);
    }

    public boolean isDone()
    {
        return (flags & DONE_CODE) != 0;
    }

    public AttemptStateFlags withSuccess()
    {
        return AttemptStateFlags.of(flags | SUCCESS_CODE);
    }

    public boolean isSuccess()
    {
        return (flags & SUCCESS_CODE) != 0;
    }

    @Override
    public int hashCode()
    {
        return flags;
    }

    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof AttemptStateFlags && ((AttemptStateFlags) another).flags == flags);
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
        if (isDone()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("DONE");
        }
        if (isSuccess()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append("SUCCESS");
        }
        return "AttemptStateFlags{" + sb.toString() + "}";
    }
}

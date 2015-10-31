package io.digdag.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class ScheduleType
{
    public static class Builder
    {
        private int flags = 0;

        public Builder groupingOnly(boolean v)
        {
            if (v) {
                flags |= SLA_TASK;
            }
            else {
                flags &= ~SLA_TASK;
            }
            return this;
        }

        public ScheduleType build()
        {
            return new ScheduleType(flags);
        }
    }

    public static final int SLA_TASK = 1;

    @JsonCreator
    public static ScheduleType of(int flags)
    {
        // TODO validation
        return new ScheduleType(flags);
    }

    private final int flags;

    public ScheduleType(int flags)
    {
        this.flags = flags;
    }

    @JsonValue
    public int get()
    {
        return flags;
    }

    public boolean isSlaTask()
    {
        return (flags & SLA_TASK) != 0;
    }

    @Override
    public int hashCode()
    {
        return flags;
    }

    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof ScheduleType && ((ScheduleType) another).flags == flags);
    }

    @Override
    public String toString()
    {
        // TODO pretty print
        return "ScheduleType{"+flags+"}";
    }
}

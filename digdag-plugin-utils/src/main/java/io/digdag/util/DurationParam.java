package io.digdag.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.time.Duration;

public class DurationParam
{
    private final Duration duration;

    private DurationParam(Duration duration)
    {
        this.duration = duration;
    }

    public Duration getDuration()
    {
        return duration;
    }

    @JsonCreator
    public static DurationParam parse(String expr)
    {
        return new DurationParam(Durations.parseDuration(expr));
    }

    public static DurationParam of(Duration duration)
    {
        return new DurationParam(duration);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DurationParam that = (DurationParam) o;

        return duration != null ? duration.equals(that.duration) : that.duration == null;
    }

    @Override
    public int hashCode()
    {
        return duration != null ? duration.hashCode() : 0;
    }

    @Override
    @JsonValue
    public String toString()
    {
        return Durations.formatDuration(duration);
    }
}

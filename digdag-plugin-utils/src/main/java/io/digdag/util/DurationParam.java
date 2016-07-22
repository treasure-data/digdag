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

    @Override
    @JsonValue
    public String toString()
    {
        return Durations.formatDuration(duration);
    }

    public static DurationParam of(Duration duration)
    {
        return new DurationParam(duration);
    }
}

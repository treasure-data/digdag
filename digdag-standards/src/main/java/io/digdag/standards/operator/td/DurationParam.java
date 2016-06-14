package io.digdag.standards.operator.td;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.digdag.util.Durations;

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
        return duration.toString();
    }

    public static DurationParam of(Duration duration)
    {
        return new DurationParam(duration);
    }
}

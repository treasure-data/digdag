package io.digdag.standards.operator.td;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.digdag.client.api.LocalTimeOrInstant;

import java.time.Instant;

public class TimestampParam
{
    private final LocalTimeOrInstant timestamp;

    private TimestampParam(LocalTimeOrInstant timestamp)
    {
        this.timestamp = timestamp;
    }

    public LocalTimeOrInstant getTimestamp()
    {
        return timestamp;
    }

    @JsonCreator
    public static TimestampParam parse(String expr)
    {
        LocalTimeOrInstant timestamp;
        if (expr.chars().allMatch(Character::isDigit)) {
            long epoch = Long.parseLong(expr);
            timestamp = LocalTimeOrInstant.of(Instant.ofEpochSecond(epoch));
        }
        else {
            timestamp = LocalTimeOrInstant.fromString(expr);
        }
        return new TimestampParam(timestamp);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return timestamp.toString();
    }

    public static TimestampParam of(LocalTimeOrInstant timestamp)
    {
        return new TimestampParam(timestamp);
    }
}

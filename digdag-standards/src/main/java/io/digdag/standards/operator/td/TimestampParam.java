package io.digdag.standards.operator.td;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
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
    public static TimestampParam parse(JsonNode expr)
    {
        LocalTimeOrInstant timestamp;
        if (expr.isTextual()) {
            if (expr.asText().chars().allMatch(Character::isDigit)) {
                timestamp = LocalTimeOrInstant.of(Instant.ofEpochSecond(Long.parseLong(expr.asText())));
            }
            else {
                timestamp = LocalTimeOrInstant.fromString(expr.asText());
            }
        }
        else if (expr.isIntegralNumber()) {
            timestamp = LocalTimeOrInstant.of(Instant.ofEpochSecond(expr.asLong()));
        }
        else {
            throw new IllegalArgumentException("Not a valid timestamp: '" + expr + "'");
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

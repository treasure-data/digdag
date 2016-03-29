package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SessionTimeTruncate
{
    SECOND("second"),
    MINUTE("minute"),
    HOUR("hour"),
    DAY("day"),
    SCHEDULE("schedule"),
    NEXT_SCHEDULE("next_schedule");

    private final String name;

    private SessionTimeTruncate(String name)
    {
        this.name = name;
    }

    @JsonCreator
    public static SessionTimeTruncate fromString(String name)
    {
        switch(name) {
        case "day":
            return DAY;
        case "hour":
            return HOUR;
        case "schedule":
            return SCHEDULE;
        case "next_schedule":
            return NEXT_SCHEDULE;
        default:
            throw new IllegalArgumentException("Unknown session time truncate option: " + name);
        }
    }

    @JsonValue
    public String toString()
    {
        return name;
    }
}

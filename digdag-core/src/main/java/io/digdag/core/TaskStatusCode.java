package io.digdag.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TaskStatusCode
{
    BLOCKED(0),
    PLANNED_RETRY_BLOCKED(1),
    READY(2),
    //
    RETRY_WAITING(4),
    PLANNED_RETRY_WAITING(5),
    RUNNING(6),
    PLANNED(7),
    RUN_ERROR(8),
    PLANNED_CHILD_ERROR(9),
    //
    SUCCESS(11),
    CANCELED(12),
    PLANNED_CANCELED(13);

    // don't call directly. called only from jackson
    @JsonCreator
    @Deprecated
    static TaskStatusCode fromJson(int code)
    {
        switch(code) {
        case 0:
        default:
            throw new IllegalStateException("Unknown task status code");
        }
    }

    private final int code;

    private TaskStatusCode(int code)
    {
        this.code = code;
    }

    public int get()
    {
        return code;
    }
}

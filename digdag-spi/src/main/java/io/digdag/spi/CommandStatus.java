package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface CommandStatus
{
    boolean isFinished();

    int getStatusCode();

    String getIoDirectory(); //relative

    @JsonValue
    ObjectNode toJson();
}
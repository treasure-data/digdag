package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface CommandStatus
{
    /**
     * Check command is finished or not.
     *
     * @return
     */
    boolean isFinished();

    /**
     * Return exit code of command finished. It7s valid only when isFiished returns true.
     * @return
     */
    int getStatusCode();

    /**
     * Return the same String with CommandRequest.getIoDirectory.
     *
     * @return
     */
    String getIoDirectory(); //relative

    @JsonValue
    ObjectNode toJson();
}
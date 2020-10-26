package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;


public interface CommandStatus
{
    /**
     * Check command is finished or not.
     *
     * @return
     */
    boolean isFinished();

    /**
     * Return exit code of command finished. It is valid only when isFinished returns true.
     * @return
     */
    int getStatusCode();

    /**
     * Return error message.
     * @return
     */
    default Optional<String> getErrorMessage()
    {
        return Optional.absent();
    }

    /**
     * Return the same String with CommandRequest.getIoDirectory.
     *
     * @return
     */
    String getIoDirectory(); //relative

    @JsonValue
    ObjectNode toJson();
}
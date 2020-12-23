package io.digdag.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.client.config.Config;

import java.io.IOException;

public interface CommandExecutor
{
    /**
     * Starts a command and returns its state. Caller must check isFinished of the returned CommandStatus.
     * If it's true, the command is done. CommandStatus.getStatusCode() is ready to call. Otherwise,
     * caller must call poll method repeatedly with toJson of the returned CommandStatus until CommandStatus
     * is returned with isFinished == true.
     *
     * @param context
     * @param request
     * @return
     * @throws IOException
     */
    CommandStatus run(CommandContext context, CommandRequest request)
            throws IOException;

    /**
     * Polls the command status by non-blocking and return CommandStatus.
     *
     * @param context
     * @param previousStatusJson
     * @return
     * @throws IOException
     */
    CommandStatus poll(CommandContext context, ObjectNode previousStatusJson)
            throws IOException;

    /**
     * Runs a cleanup script when an attempt gets CANCEL_REQUESTED.
     *
     * @param context
     * @param state
     * @throws IOException
     */
    default void cleanup(CommandContext context, Config state)
            throws IOException
    { }
}

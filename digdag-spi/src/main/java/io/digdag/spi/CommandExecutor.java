package io.digdag.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

public interface CommandExecutor
{
    /**
     * Run a command.
     *
     * @param context
     * @param request
     * @return
     * @throws IOException
     */
    CommandStatus run(CommandContext context, CommandRequest request)
            throws IOException;

    /**
     * Poll the command status by non-blocking
     * @param context
     * @param previousStatusJson
     * @return
     * @throws IOException
     */
    CommandStatus poll(CommandContext context, ObjectNode previousStatusJson)
            throws IOException;
}
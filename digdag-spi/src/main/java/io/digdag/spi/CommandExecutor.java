package io.digdag.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Path;

public interface CommandExecutor
{
    // TODO Will be removed before 0.10.x 'rb' and 'sh' operator are still using it.
    @Deprecated
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    /**
     * Run a command.
     *
     * @param context
     * @param request
     * @return
     * @throws IOException
     */
    CommandStatus run(CommandExecutorContext context,
            CommandExecutorRequest request)
            throws IOException;

    /**
     * Poll the command status by non-blocking
     * @param context
     * @param previousStatusJson
     * @return
     * @throws IOException
     */
    CommandStatus poll(CommandExecutorContext context,
            ObjectNode previousStatusJson)
            throws IOException;


}
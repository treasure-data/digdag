package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandStatus;

import java.util.Map;

public class ProcessCommandStatus
        extends CommandStatus
{
    private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

    // called from ProcessCommandExecutor
    static ProcessCommandStatus of(final int statusCode,
            final Map<String, CommandExecutorContent> outputContents)
    {
        final ObjectNode object = FACTORY.objectNode();
        // "is_finished" key is not necessary because isFinished method is overridden and always returns "true".
        object.set("status_code", FACTORY.numberNode(statusCode));
        return new ProcessCommandStatus(object, outputContents);
    }

    private final Map<String, CommandExecutorContent> outputContents;

    private ProcessCommandStatus(final ObjectNode object,
            final Map<String, CommandExecutorContent> outputContents)
    {
        super(object);
        this.outputContents = outputContents;
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public CommandExecutorContent getOutputContent(String path)
    {
        return outputContents.get(path);
    }
}
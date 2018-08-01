package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.spi.CommandStatus;

class DefaultCommandStatus
        implements CommandStatus
{
    static CommandStatus fromObjectNode(final ObjectNode object)
    {
        return new DefaultCommandStatus(object);
    }

    private final ObjectNode object;

    private DefaultCommandStatus(final ObjectNode object)
    {
        this.object = object;
    }

    @Override
    public boolean isFinished()
    {
        return object.get("finished").asBoolean();
    }

    @Override
    public int getStatusCode()
    {
        return object.get("status_code").intValue();
    }

    @Override
    public String getIoDirectory()
    {
        return object.get("io_directory").textValue();
    }

    @Override
    public ObjectNode toJson()
    {
        return object;
    }
}

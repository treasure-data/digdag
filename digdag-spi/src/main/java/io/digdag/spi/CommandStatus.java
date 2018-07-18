package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class CommandStatus
{
    // This method is used to deserialize from state params in operators.
    @JsonCreator
    public static CommandStatus deserializeFromObjectNode(final ObjectNode object)
    {
        return new CommandStatus(object);
    }

    protected final ObjectNode object;

    protected CommandStatus(final ObjectNode object)
    {
        this.object = object.deepCopy();
    }

    public boolean isFinished()
    {
        if (object.has("is_finished")) {
            return object.get("is_finished").asBoolean();
        }
        return false;
    }

    public Optional<Integer> getStatusCode()
    {
        if (object.has("status_code")) {
            final int i = object.get("status_code").asInt(Integer.MAX_VALUE);
            if (Integer.MAX_VALUE > i) {
                return Optional.of(i);
            }
        }
        return Optional.absent();
    }

    @JsonValue
    public ObjectNode getObjectNode()
    {
        return object;
    }

    @Override
    public String toString()
    {
        return object.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof CommandStatus)) {
            return false;
        }
        return object.equals(((CommandStatus) other).object);
    }

    @Override
    public int hashCode()
    {
        return object.hashCode();
    }

    public CommandExecutorContent getOutputContent(String path) // {path => content}
    {
        throw new UnsupportedOperationException("This method must be overridden by sub-classes.");
    }
}
package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.spi.CommandStatus;

public class KubernetesCommandStatus
        implements CommandStatus
{
    static KubernetesCommandStatus of(final boolean isFinished,
            final ObjectNode json)
    {
        return new KubernetesCommandStatus(isFinished, json);
    }

    private final boolean isFinished;
    private final ObjectNode json;

    KubernetesCommandStatus(final boolean isFinished,
            final ObjectNode json)
    {
        this.isFinished = isFinished;
        this.json = json;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public int getStatusCode()
    {
        return json.get("status_code").intValue();
    }

    @Override
    public String getIoDirectory()
    {
        return json.get("io_directory").textValue();
    }

    @Override
    public ObjectNode toJson()
    {
        return json;
    }
}

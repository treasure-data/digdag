package io.digdag.core.agent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class AgentId
{
    @JsonCreator
    public static AgentId of(String id)
    {
        return new AgentId(id);
    }

    private final String id;

    private AgentId(String id)
    {
        this.id = id;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof AgentId)) {
            return false;
        }
        AgentId o = (AgentId) obj;
        return id.equals(o.id);
    }
}

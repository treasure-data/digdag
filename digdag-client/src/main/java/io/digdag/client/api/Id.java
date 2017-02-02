package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface Id
{
    String get();

    static Id of(String id)
    {
        return new ImmutableId(id);
    }

    // This method is only for backward compatibility.
    // @JsonCreator can be moved to of(String).
    @JsonCreator
    @Deprecated
    static Id fromJson(JsonNode node)
            throws JsonMappingException
    {
        if (node.isTextual() || node.isNumber()) {
            return of(node.asText());
        }
        else {
            throw new JsonMappingException("Invalid ID. Expected string but got " + node);
        }
    }

    default int asInt()
        throws NumberFormatException
    {
        return Integer.parseInt(get());
    }

    default long asLong()
        throws NumberFormatException
    {
        return Long.parseLong(get());
    }
}

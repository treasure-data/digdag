package io.digdag.client.api;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonMappingException;

@Value.Immutable
public interface RestDirectDownloadHandle
{
    @JsonValue
    String getUrl();

    @JsonCreator
    @Deprecated
    static RestDirectDownloadHandle of(JsonNode node)
        throws JsonMappingException
    {
        String url;
        if (node.isTextual()) {
            url = node.asText();
        }
        else if (node.isObject()) {
            // This code is only for backward compatibility.
            // This should be removed at next next release.
            ObjectNode obj = (ObjectNode) node;
            JsonNode v = obj.get("url");
            if (v.isTextual()) {
                url = v.asText();
            }
            else {
                throw new JsonMappingException("Can not deserialize instance of RestDirectDownloadHandle out of malformed object");
            }
        }
        else {
            throw new JsonMappingException("Can not deserialize instance of RestDirectDownloadHandle out of malformed node");
        }
        return of(url);
    }

    static RestDirectDownloadHandle of(String url)
    {
        return ImmutableRestDirectDownloadHandle.builder()
            .url(url)
            .build();
    }
}

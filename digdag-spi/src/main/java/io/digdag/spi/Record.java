package io.digdag.spi;

import com.fasterxml.jackson.databind.JsonNode;
import org.immutables.value.Value;

@Value.Immutable
public interface Record
{
    String key();

    JsonNode value(); // {"value": 1}, {"value": [1,2,3,4]}

    ValueType valueType();

    static ImmutableRecord.Builder builder()
    {
        return ImmutableRecord.builder();
    }
}

package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSessionCollection.class)
public interface RestSessionCollection
{
    List<RestSession> getSessions();

    int getRecordsNumber();

    int getPageSize();

    static ImmutableRestSessionCollection.Builder builder()
    {
        return ImmutableRestSessionCollection.builder();
    }
}

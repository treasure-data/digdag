package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestRevisionCollection.class)
public interface RestRevisionCollection
{
    List<RestRevision> getRevisions();

    static ImmutableRestRevisionCollection.Builder builder()
    {
        return ImmutableRestRevisionCollection.builder();
    }
}

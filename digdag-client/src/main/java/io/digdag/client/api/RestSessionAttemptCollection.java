package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSessionAttemptCollection.class)
public interface RestSessionAttemptCollection
{
    List<RestSessionAttempt> getAttempts();

    static ImmutableRestSessionAttemptCollection.Builder builder()
    {
        return ImmutableRestSessionAttemptCollection.builder();
    }
}

package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskRequestCollection.class)
public interface RestTaskRequestCollection
{
    List<Config> getTaskRequests();

    static ImmutableRestTaskRequestCollection.Builder builder()
    {
        return ImmutableRestTaskRequestCollection.builder();
    }
}

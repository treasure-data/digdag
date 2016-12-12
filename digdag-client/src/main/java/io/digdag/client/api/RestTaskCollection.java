package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskCollection.class)
public interface RestTaskCollection
{
    List<RestTask> getTasks();

    static ImmutableRestTaskCollection.Builder builder()
    {
        return ImmutableRestTaskCollection.builder();
    }
}

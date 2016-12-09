package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestProjectCollection.class)
public interface RestProjectCollection
{
    List<RestProject> getProjects();

    static ImmutableRestProjectCollection.Builder builder()
    {
        return ImmutableRestProjectCollection.builder();
    }
}

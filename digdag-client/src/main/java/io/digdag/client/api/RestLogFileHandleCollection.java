package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestLogFileHandleCollection.class)
public interface RestLogFileHandleCollection
{
    List<RestLogFileHandle> getFiles();

    static ImmutableRestLogFileHandleCollection.Builder builder()
    {
        return ImmutableRestLogFileHandleCollection.builder();
    }
}

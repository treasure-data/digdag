package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSessionAttemptCollection.class)
public interface RestScheduleAttemptCollection
{
    Id getId();

    IdAndName getProject();

    IdAndName getWorkflow();

    List<RestSessionAttempt> getAttempts();

    static ImmutableRestScheduleAttemptCollection.Builder builder()
    {
        return ImmutableRestScheduleAttemptCollection.builder();
    }
}

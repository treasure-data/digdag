package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskPollRequest.class)
public interface RestTaskPollRequest
{
    Optional<String> getQueueName();

    String getAgentId();

    int getCount();

    int getMaxSleepMillis();

    int getLockSeconds();

    static ImmutableRestTaskPollRequest.Builder builder()
    {
        return ImmutableRestTaskPollRequest.builder();
    }
}

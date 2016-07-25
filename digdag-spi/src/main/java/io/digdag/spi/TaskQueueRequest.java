package io.digdag.spi;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableTaskQueueRequest.class)
public interface TaskQueueRequest
    extends TaskQueueData
{
    Optional<String> getQueueName();

    int getPriority();

    static ImmutableTaskQueueRequest.Builder builder()
    {
        return ImmutableTaskQueueRequest.builder();
    }
}

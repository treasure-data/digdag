package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskSucceededCallback.class)
public interface RestTaskSucceededCallback
{
    String getLockId();

    String getAgentId();

    Config getTaskResult();
}

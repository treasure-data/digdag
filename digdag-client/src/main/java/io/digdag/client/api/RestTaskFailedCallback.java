package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskFailedCallback.class)
public interface RestTaskFailedCallback
{
    String getLockId();

    String getAgentId();

    Config getError();

    static ImmutableRestTaskFailedCallback.Builder builder()
    {
        return ImmutableRestTaskFailedCallback.builder();
    }
}

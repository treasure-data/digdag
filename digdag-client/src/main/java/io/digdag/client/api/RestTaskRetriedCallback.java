package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskRetriedCallback.class)
public interface RestTaskRetriedCallback
{
    String getLockId();

    String getAgentId();

    int getRetryInterval();

    Config getRetryStateParams();

    Optional<Config> getError();
}

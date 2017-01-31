package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTaskHeartbeatCallback.class)
public interface RestTaskHeartbeatCallback
{
    List<String> getLockIds();

    String getAgentId();

    int getLockSeconds();
}

package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestVersionCheckResult.class)
public interface RestVersionCheckResult
{
    String getServerVersion();

    boolean getUpgradeRecommended();

    boolean getApiCompatible();

    static ImmutableRestVersionCheckResult.Builder builder()
    {
        return ImmutableRestVersionCheckResult.builder();
    }
}

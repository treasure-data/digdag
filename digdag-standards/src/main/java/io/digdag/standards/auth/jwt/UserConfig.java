package io.digdag.standards.auth.jwt;

import io.digdag.client.api.RestApiKey;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableUserConfig.class)
@JsonDeserialize(as = ImmutableUserConfig.class)
public abstract class UserConfig
{
    public abstract int getSiteId();

    public abstract RestApiKey getApiKey();

    public abstract boolean isAdmin();

    public static ImmutableUserConfig.Builder builder()
    {
        return ImmutableUserConfig.builder();
    }
}

package io.digdag.standards.auth.jwt;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.Map;

@Value.Immutable
@JsonSerialize(as = ImmutableJwtAuthenticatorConfig.class)
@JsonDeserialize(as = ImmutableJwtAuthenticatorConfig.class)
public abstract class JwtAuthenticatorConfig
{
    public abstract Map<String, UserConfig> getUserMap();

    public abstract boolean isAllowPublicAccess();

    public static ImmutableJwtAuthenticatorConfig.Builder builder() {
        return ImmutableJwtAuthenticatorConfig.builder();
    }
}

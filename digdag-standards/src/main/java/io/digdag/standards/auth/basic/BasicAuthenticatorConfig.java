package io.digdag.standards.auth.basic;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBasicAuthenticatorConfig.class)
@JsonDeserialize(as = ImmutableBasicAuthenticatorConfig.class)
public abstract class BasicAuthenticatorConfig
{
    public abstract String getUsername();
    public abstract String getPassword();
    public abstract boolean isAdmin();

    public static ImmutableBasicAuthenticatorConfig.Builder builder() {
        return ImmutableBasicAuthenticatorConfig.builder();
    }
}

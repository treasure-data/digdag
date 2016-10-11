package io.digdag.core.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretAccessPolicy;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GrantedPrivilegedVariables
    implements PrivilegedVariables
{
    static SecretProvider privilegedSecretProvider(SecretAccessContext context, SecretAccessPolicy accessPolicy, SecretStore store)
    {
        return (key) -> {
            if (!accessPolicy.isSecretAccessible(context, key)) {
                return Optional.absent();
            }

            Optional<String> projectSecret = store.getSecret(context.projectId(), SecretScopes.PROJECT, key);

            if (projectSecret.isPresent()) {
                return projectSecret;
            }

            return store.getSecret(context.projectId(), SecretScopes.PROJECT_DEFAULT, key);
        };
    }

    private static class SecretOnlyGrant
    {
        @JsonProperty
        ConfigKey secret;
    }

    private interface VariableAccessor
    {
        String get(boolean required);
    }

    public static GrantedPrivilegedVariables empty()
    {
        return new GrantedPrivilegedVariables(new LinkedHashMap<>());
    }

    public static GrantedPrivilegedVariables build(
            Config grants,
            Config params,
            SecretProvider secretProvider)
    {
        Map<String, VariableAccessor> variables = new LinkedHashMap<>();
        for (String key : grants.getKeys()) {
            variables.put(key, buildAccessor(grants, key, params, secretProvider));
        }
        return new GrantedPrivilegedVariables(variables);
    }

    private static VariableAccessor buildAccessor(
            Config grants, String key,
            Config params,
            SecretProvider secretProvider)
    {
        if (grants.get(key, JsonNode.class).isObject()) {
            ConfigKey secretOnlyKey = grants.getNested(key).convert(SecretOnlyGrant.class).secret;
            return (required) -> {
                Optional<String> secret = secretProvider.getSecretOptional(secretOnlyKey.toString());
                if (required && !secret.isPresent()) {
                    throw new SecretNotFoundException(secretOnlyKey.toString());
                }
                return secret.orNull();
            };
        }
        else {
            ConfigKey secretSharedKey = grants.get(key, ConfigKey.class);
            return (required) -> {
                Optional<String> secret = secretProvider.getSecretOptional(secretSharedKey.toString());
                if (secret.isPresent()) {
                    return secret.get();
                }

                Config nested = params;
                for (String nestName : secretSharedKey.getNestNames()) {
                    Optional<Config> optionalNested = nested.getOptionalNested(nestName);
                    if (!optionalNested.isPresent()) {
                        if (required) {
                            throw new ConfigException("Nested object '" + nestName + "' out of " + secretSharedKey + " is required but not set");
                        }
                        else {
                            return null;
                        }
                    }
                    nested = optionalNested.get();
                }
                String value = nested.get(secretSharedKey.getLastName(), String.class, null);
                if (required && value == null) {
                    throw new ConfigException("Nested object '" + secretSharedKey.getLastName() + "' out of " + secretSharedKey + " is required but not set");
                }
                return value;
            };
        }
    }

    private final Map<String, VariableAccessor> variables;

    private GrantedPrivilegedVariables(
            Map<String, VariableAccessor> variables)
    {
        this.variables = variables;
    }

    @Override
    public String get(String key)
    {
        VariableAccessor var = variables.get(key);
        if (var == null) {
            return null;
        }
        else {
            return var.get(true);
        }
    }

    @Override
    public Optional<String> getOptional(String key)
    {
        VariableAccessor var = variables.get(key);
        if (var == null) {
            return Optional.absent();
        }
        else {
            return Optional.fromNullable(var.get(false));
        }
    }

    @Override
    public List<String> getKeys()
    {
        return ImmutableList.copyOf(variables.keySet());
    }
}

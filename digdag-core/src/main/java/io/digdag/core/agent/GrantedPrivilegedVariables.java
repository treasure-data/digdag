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
import io.digdag.util.UserSecretTemplate;
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
        String get();
    }

    public static GrantedPrivilegedVariables empty()
    {
        return new GrantedPrivilegedVariables(new LinkedHashMap<>());
    }

    public static GrantedPrivilegedVariables build(Config source, SecretProvider secretProvider)
    {
        Map<String, VariableAccessor> variables = new LinkedHashMap<>();
        for (String key : source.getKeys()) {
            variables.put(key, buildAccessor(source.get(key, String.class), secretProvider));
        }
        return new GrantedPrivilegedVariables(variables);
    }

    private static VariableAccessor buildAccessor(String template, SecretProvider secretProvider)
    {
        return () -> UserSecretTemplate.of(template).format(secretProvider);
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
            throw new ConfigException("_env variable '" + key + "' is required but not set");
        }
        else {
            String value = var.get();
            if (value == null) {
                throw new ConfigException("_env variable '" + key + "' is required but null");
            }
            return value;
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
            return Optional.fromNullable(var.get());
        }
    }

    @Override
    public List<String> getKeys()
    {
        return ImmutableList.copyOf(variables.keySet());
    }
}

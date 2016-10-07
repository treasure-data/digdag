package io.digdag.core.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.PrivilegedVariables;
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
    static SecretProvider privilegedSecretProvider(SecretStore secretStore, int projectId)
    {
        return (key) -> {
            Optional<String> projectSecret = secretStore.getSecret(projectId, SecretScopes.PROJECT, key);

            if (projectSecret.isPresent()) {
                return projectSecret;
            }

            return secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, key);
        };
    }

    private static class SecretOnlyGrant
    {
        @JsonProperty
        String secret;
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
            String secretOnlyKey = grants.getNested(key).convert(SecretOnlyGrant.class).secret;
            return (required) -> {
                Optional<String> secret = secretProvider.getSecretOptional(secretOnlyKey);
                if (required && !secret.isPresent()) {
                    throw new SecretNotFoundException(secretOnlyKey);
                }
                return secret.orNull();
            };
        }
        else {
            String secretSharedKey = grants.get(key, String.class);
            return (required) -> {
                Optional<String> secret = secretProvider.getSecretOptional(secretSharedKey);
                if (secret.isPresent()) {
                    return secret.get();
                }

                // TODO use ConfigKey class added at #336 to access nested key
                List<String> configKey = ImmutableList.copyOf(secretSharedKey.split("\\."));
                Config config = params;
                for (String nestName : configKey.subList(0, configKey.size() - 1)) {
					Optional<Config> nest = config.getOptionalNested(nestName);
					if (!nest.isPresent()) {
                        break;
					}
					config = nest.get();
                }
                String name = configKey.get(configKey.size() - 1);
                if (required) {
                    return config.get(name, String.class);
                }
                else {
                    return config.get(name, String.class, null);
                }
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

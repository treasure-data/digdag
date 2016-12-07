package io.digdag.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.digdag.spi.SecretAccessList;
import java.util.List;
import java.util.Set;

public class ConfigSelector
    implements SecretAccessList
{
    private final List<String> scopes;
    private final Set<String> secretAccessKeys;
    private final Set<String> secretOnlyAccessKeys;

    private ConfigSelector(
            List<String> scopes,
            Set<String> secretAccessKeys,
            Set<String> secretOnlyAccessKeys)
    {
        this.scopes = scopes;
        this.secretAccessKeys = secretAccessKeys;
        this.secretOnlyAccessKeys = secretOnlyAccessKeys;
    }

    public String getPrimaryScope()
    {
        return scopes.get(0);
    }

    @Override
    public Set<String> getSecretKeys()
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        set.addAll(secretAccessKeys);
        set.addAll(secretOnlyAccessKeys);
        return set.build();
    }

    public Set<String> getSecretAccessList()
    {
        return secretAccessKeys;
    }

    public Set<String> getSecretOnlyAccessList()
    {
        return secretOnlyAccessKeys;
    }

    public ConfigSelector withExtraSecretAccessList(ConfigSelector another)
    {
        ImmutableList.Builder<String> scopeBuilder = ImmutableList.builder();
        ImmutableSet.Builder<String> secretAccessBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<String> secretOnlyAccessBuilder = ImmutableSet.builder();

        scopeBuilder.addAll(scopes);
        secretAccessBuilder.addAll(secretAccessKeys);
        secretOnlyAccessBuilder.addAll(secretOnlyAccessKeys);

        scopeBuilder.addAll(another.scopes);
        secretAccessBuilder.addAll(another.secretAccessKeys);
        secretOnlyAccessBuilder.addAll(another.secretOnlyAccessKeys);

        return new ConfigSelector(
                scopeBuilder.build(),
                secretAccessBuilder.build(),
                secretOnlyAccessBuilder.build());
    }

    public static Builder builderOfScope(String scope)
    {
        return new Builder(scope);
    }

    public static class Builder
    {
        private final String scope;
        private ImmutableSet.Builder<String> secretAccessBuilder = ImmutableSet.builder();
        private ImmutableSet.Builder<String> secretOnlyAccessBuilder = ImmutableSet.builder();

        private Builder(String scope)
        {
            this.scope = scope;
        }

        private Builder addSecretAccess(String key)
        {
            Preconditions.checkNotNull(key);
            secretAccessBuilder.add(scope + "." + key);
            return this;
        }

        public Builder addSecretAccess(String... keys)
        {
            for (String key : keys) {
                addSecretAccess(key);
            }
            return this;
        }

        public Builder addAllSecretAccess(Iterable<? extends String> keys)
        {
            for (String key : keys) {
                addSecretAccess(key);
            }
            return this;
        }

        private Builder addSecretOnlyAccess(String key)
        {
            Preconditions.checkNotNull(key);
            secretOnlyAccessBuilder.add(scope + "." + key);
            return this;
        }

        public Builder addSecretOnlyAccess(String... keys)
        {
            for (String key : keys) {
                addSecretOnlyAccess(key);
            }
            return this;
        }

        public Builder addAllSecretOnlyAccess(Iterable<? extends String> keys)
        {
            for (String key : keys) {
                addSecretOnlyAccess(key);
            }
            return this;
        }

        public ConfigSelector build()
        {
            return new ConfigSelector(
                    ImmutableList.of(scope),
                    secretAccessBuilder.build(),
                    secretOnlyAccessBuilder.build());
        }
    }
}

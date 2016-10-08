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
    private final Set<String> secretOnlyAccessKeys;
    private final Set<String> secretSharedAccessKeys;

    private ConfigSelector(
            List<String> scopes,
            Set<String> secretOnlyAccessKeys,
            Set<String> secretSharedAccessKeys)
    {
        this.scopes = scopes;
        this.secretOnlyAccessKeys = secretOnlyAccessKeys;
        this.secretSharedAccessKeys = secretSharedAccessKeys;
    }

    public String getPrimaryScope()
    {
        return scopes.get(0);
    }

    @Override
    public Set<String> getSecretKeys()
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        set.addAll(secretOnlyAccessKeys);
        set.addAll(secretSharedAccessKeys);
        return set.build();
    }

    public Set<String> getSecretOnlyAccessList()
    {
        return secretOnlyAccessKeys;
    }

    public Set<String> getSecretSharedAccessList()
    {
        return secretSharedAccessKeys;
    }

    public ConfigSelector withExtraSecretAccessList(ConfigSelector another)
    {
        ImmutableList.Builder<String> scopeBuilder = ImmutableList.builder();
        ImmutableSet.Builder<String> secretOnlyAccessBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<String> secretSharedAccessBuilder = ImmutableSet.builder();

        scopeBuilder.addAll(scopes);
        secretOnlyAccessBuilder.addAll(secretOnlyAccessKeys);
        secretSharedAccessBuilder.addAll(secretSharedAccessKeys);

        scopeBuilder.addAll(another.scopes);
        secretOnlyAccessBuilder.addAll(another.secretOnlyAccessKeys);
        secretSharedAccessBuilder.addAll(another.secretSharedAccessKeys);

        return new ConfigSelector(
                scopeBuilder.build(),
                secretOnlyAccessBuilder.build(),
                secretSharedAccessBuilder.build());
    }

    public static Builder builderOfScope(String scope)
    {
        return new Builder(scope);
    }

    public static class Builder
    {
        private final String scope;
        private ImmutableSet.Builder<String> secretOnlyAccessBuilder = ImmutableSet.builder();
        private ImmutableSet.Builder<String> secretSharedAccessBuilder = ImmutableSet.builder();

        private Builder(String scope)
        {
            this.scope = scope;
        }

        public Builder addSecretOnlyAccess(String key)
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

        public Builder addSecretSharedAccess(String key)
        {
            Preconditions.checkNotNull(key);
            secretSharedAccessBuilder.add(scope + "." + key);
            return this;
        }

        public Builder addSecretSharedAccess(String... keys)
        {
            for (String key : keys) {
                addSecretSharedAccess(key);
            }
            return this;
        }

        public Builder addAllSecretSharedAccess(Iterable<? extends String> keys)
        {
            for (String key : keys) {
                addSecretSharedAccess(key);
            }
            return this;
        }

        public ConfigSelector build()
        {
            return new ConfigSelector(
                    ImmutableList.of(scope),
                    secretOnlyAccessBuilder.build(),
                    secretSharedAccessBuilder.build());
        }
    }
}

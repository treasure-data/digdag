package io.digdag.util;

import java.util.Collection;
import java.util.Set;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.spi.SecretProvider;

public class ConfigScope
{
    private final String scope;
    private final Config scopedConfig;
    private final Config templateParams;
    private final SecretProvider secrets;
    private final Set<String> secretOnlyScopedKeys;
    private final Set<String> secretSharedScopedKeys;

    ConfigScope(
            String scope,
            Config localConfig,
            Config runtimeParams,
            ConfigSelector configSelector,
            SecretProvider secrets)
    {
        this.scope = scope;

        Config scopedRuntimeParams = runtimeParams.getNestedOrGetEmpty(scope);
        this.scopedConfig = localConfig.deepCopy().mergeDefault(scopedRuntimeParams);
        this.templateParams = localConfig.deepCopy().mergeDefault(scopedRuntimeParams).mergeDefault(runtimeParams);

        this.secrets = secrets;
        this.secretOnlyScopedKeys = buildScopedKeys(scope, configSelector.getSecretOnlyAccessList());
        this.secretSharedScopedKeys = buildScopedKeys(scope, configSelector.getSecretSharedAccessList());
    }

    private static Set<String> buildScopedKeys(String scope, Collection<String> keys)
    {
        String prefix = scope + ".";
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key : keys) {
            if (key.startsWith(prefix)) {
                builder.add(key.substring(prefix.length()));
            }
        }
        return builder.build();
    }

    private Config getScopedConfig()
    {
        return scopedConfig;
    }

    public Config getTemplateParams()
    {
        return templateParams;
    }

    public <E> E get(String scopedKey, Class<E> klass)
    {
        Optional<String> secret = getSecret(scopedKey);
        if (secret.isPresent()) {
            return convertString(scopedKey, secret.get(), klass);
        }
        else if (isSecretOnly(scopedKey)) {
            throw secretNotFound(scopedKey);
        }
        else {
            return scopedConfig.get(scopedKey, klass);
        }
    }

    public <E> Optional<E> getOptional(String scopedKey, Class<E> klass)
    {
        Optional<String> secret = getSecret(scopedKey);
        if (secret.isPresent()) {
            return Optional.of(convertString(scopedKey, secret.get(), klass));
        }
        else if (isSecretOnly(scopedKey)) {
            return Optional.absent();
        }
        else {
            return scopedConfig.getOptional(scopedKey, klass);
        }
    }

    public Config parseNested(String scopedKey)
    {
        Optional<String> secret = getSecret(scopedKey);
        if (secret.isPresent()) {
            return parseNestedString(scopedKey, secret.get());
        }
        else if (isSecretOnly(scopedKey)) {
            throw secretNotFound(scopedKey);
        }
        else {
            return scopedConfig.parseNested(scopedKey);
        }
    }

    public Config parseNestedOrGetEmpty(String scopedKey)
    {
        Optional<String> secret = getSecret(scopedKey);
        if (secret.isPresent()) {
            return parseNestedString(scopedKey, secret.get());
        }
        else if (isSecretOnly(scopedKey)) {
            return newConfig();
        }
        else {
            return scopedConfig.parseNestedOrGetEmpty(scopedKey);
        }
    }

    private boolean isSecretOnly(String scopedKey)
    {
        return secretOnlyScopedKeys.contains(scopedKey);
    }

    private SecretNotFoundException secretNotFound(String scopedKey)
    {
        return new SecretNotFoundException(scope + "." + scopedKey);
    }

    private <E> E convertString(String scopedKey, String text, Class<E> klass)
    {
        return newConfig().set(scopedKey, text).get(scopedKey, klass);
    }

    private Config parseNestedString(String scopedKey, String text)
    {
        return newConfig().set(scopedKey, text).parseNested(scopedKey);
    }

    private Optional<String> getSecret(String scopedKey)
    {
        if (secretOnlyScopedKeys.contains(scopedKey) || secretSharedScopedKeys.contains(scopedKey)) {
            String key = scope + "." + scopedKey;
            return secrets.getSecretOptional(key);
        }
        else {
            return Optional.absent();
        }
    }

    private Config newConfig()
    {
        return scopedConfig.getFactory().create();
    }
}

package io.digdag.spi;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.digdag.client.api.SecretValidation;

/**
 * A VFS style secret access interface.
 */
public interface SecretProvider
{
    /**
     * Get a secret identified by a key.
     *
     * @param key A key identifing the secret to get.
     * @return A secret.
     * @throws SecretAccessDeniedException if access to the secret was not permitted.
     * @throws SecretNotFoundException if no matching secret was found.
     */
    default String getSecret(String key) {
        return getSecretOptional(key).or(() -> {
            throw new SecretNotFoundException(key);
        });
    }

    /**
     * Get a secret identified by a key.
     *
     * @param key A key identifing the secret to get.
     * @return {@link Optional#of(Object)} with a secret or {@link Optional#absent()} if no matching secret was found.
     * @throws SecretAccessDeniedException if access to the secret was not permitted.
     */
    Optional<String> getSecretOptional(String key);

    /**
     * Get a view of a subtree of the secret VFS. All secret accesses by the returned {@link SecretProvider} are prefixed by the specified path.
     */
    default SecretProvider getSecrets(String path) {
        return new ScopedSecretProvider(this, path);
    }

    /**
     * A secret provider that implements a virtual subtree view.
     */
    class ScopedSecretProvider
            implements SecretProvider
    {
        private final SecretProvider delegate;
        private final String path;

        ScopedSecretProvider(SecretProvider delegate, String path) {
            this.delegate = Preconditions.checkNotNull(delegate, "delegate");
            this.path = Preconditions.checkNotNull(path, "path");
            Preconditions.checkArgument(SecretValidation.isValidSecretKey(path), "invalid path: %s", path);
        }

        @Override
        public Optional<String> getSecretOptional(String key)
        {
            Preconditions.checkArgument(SecretValidation.isValidSecretKey(key), "invalid key: %s", key);
            return delegate.getSecretOptional(this.path + '.' + key);
        }
    }
}

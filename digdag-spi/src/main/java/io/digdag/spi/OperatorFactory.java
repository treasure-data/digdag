package io.digdag.spi;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

public interface OperatorFactory
{
    String getType();

    /**
     * Get a list of secret secret keys that this operator intends to access.
     *
     * An attempt to access a secret using a key not covered by
     * one of these keys will result in a {@link SecretAccessDeniedException}.
     *
     * If this operator wants to use secret keys given by configuration parameters
     * (e.g. secrets.getSecrets(config.get("secret", String.class))), the keys won't be (can't be)
     * included here. Instead, users must grant the access explicitly using _secret directive.
     * Operators should test the access to the key by overriding
     * {@link Operator#testUserSecretAccess(String)} method.
     */
    default SecretAccessList getSecretAccessList()
    {
        return new SecretAccessList()
        {
            @Override
            public Set<String> getSecretKeys()
            {
                return Collections.emptySet();
            }
        };
    }

    Operator newOperator(OperatorContext context);
}

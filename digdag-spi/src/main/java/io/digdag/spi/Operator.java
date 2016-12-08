package io.digdag.spi;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface Operator
{
    TaskResult run();

    /**
     * Return false if operator doesn't need the given secret key granted explicitly by users.
     *
     * This method allows operator implementations to reject use of secrets which are explicitly
     * granted by _secret directive when the operator knows the key is actually unnecessary.
     * Operators should override this method to avoid unintentional use of secrets.
     */
    default boolean testUserSecretAccess(String key)
    {
        return true;
    }
}

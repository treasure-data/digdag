package io.digdag.spi;

import com.google.common.base.Optional;
import java.util.List;

public interface PrivilegedVariables
{
    // PrivilegedVariables fetches variables lazily.
    // Therefore, get(String key) may throw exceptions even if the key
    // is included in the result of getKeys() as following:
    //
    // * key is granted for secret-only access
    //   * secret store doesn't include key
    //     => throw SecretNotFoundException
    // * key is granted for secret-shared access
    //   * secret store doesn't include key
    //     and runtime parameters don't include key
    //     => throw ConfigException

    String get(String key);

    Optional<String> getOptional(String key);

    List<String> getKeys();
}

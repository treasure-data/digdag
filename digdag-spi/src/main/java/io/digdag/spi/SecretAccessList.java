package io.digdag.spi;

import java.util.Set;

public interface SecretAccessList
{
    Set<String> getSecretKeys();
}

package io.digdag.spi;

import java.util.List;

public interface SecretControlStore
{
    void setProjectSecret(int projectId, String scope, String key, String value);

    void deleteProjectSecret(int projectId, String scope, String key);

    List<String> listProjectSecrets(int projectId, String scope);
}

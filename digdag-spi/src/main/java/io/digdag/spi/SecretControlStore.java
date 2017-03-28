package io.digdag.spi;

import com.google.common.base.Optional;
import java.util.List;

public interface SecretControlStore
{
    void setProjectSecret(int projectId, String scope, String key, String value);

    void deleteProjectSecret(int projectId, String scope, String key);

    List<String> listProjectSecrets(int projectId, String scope);

    interface SecretLockAction <T>
    {
        T call(SecretControlStore store, Optional<String> lockedValue);
    }

    <T> T lockProjectSecret(int projectId, String scope, String key, SecretLockAction<T> action);
}

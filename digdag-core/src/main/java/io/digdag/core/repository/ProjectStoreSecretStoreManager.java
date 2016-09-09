package io.digdag.core.repository;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;

public class ProjectStoreSecretStoreManager
        implements SecretStoreManager
{
    private final ProjectStoreManager projectStoreManager;
    private final SecretCrypto crypto;

    @Inject
    public ProjectStoreSecretStoreManager(ProjectStoreManager projectStoreManager, SecretCrypto crypto)
    {
        this.projectStoreManager = projectStoreManager;
        this.crypto = crypto;
    }

    @Override
    public SecretStore getSecretStore(int siteId)
    {
        return new RepositorySecretStore(projectStoreManager.getProjectStore(siteId));
    }

    private class RepositorySecretStore
            implements SecretStore
    {
        private final ProjectStore projectStore;

        public RepositorySecretStore(ProjectStore projectStore)
        {
            this.projectStore = projectStore;
        }

        @Override
        public Optional<String> getSecret(int projectId, String scope, String key)
        {
            return projectStore.getSecretIfExists(projectId, scope, key);
        }
    }
}

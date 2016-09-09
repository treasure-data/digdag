package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreSecretStoreManager;
import io.digdag.core.repository.StoredProject;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DatabaseProjectStoreManagerSecretsTest
{
    private static final int SITE_ID = 17;

    private static final String KEY1 = "foo1";
    private static final String KEY2 = "foo2";
    private static final String VALUE1 = "bar1";
    private static final String VALUE2 = "bar2";
    private static final String VALUE3 = "bar3";

    private DatabaseFactory factory;
    private StoredProject storedProject;
    private ProjectStore projectStore;

    private SecretStore secretStore;
    private int projectId;

    @Before
    public void setUp()
            throws Exception
    {
        factory = DatabaseTestingUtils.setupDatabase();

        SecretCrypto secretCrypto = factory.newRandomSecretCrypto();

        DatabaseProjectStoreManager projectStoreManager = factory.getProjectStoreManager(secretCrypto);
        projectStore = projectStoreManager.getProjectStore(SITE_ID);

        Project project = Project.of("proj1");
        storedProject = projectStore.putAndLockProject(project, (store, stored) -> stored);

        ProjectStoreSecretStoreManager secretStoreManager = new ProjectStoreSecretStoreManager(projectStoreManager, secretCrypto);
        secretStore = secretStoreManager.getSecretStore(SITE_ID);

        projectId = storedProject.getId();
    }

    @Test
    public void noSecret()
            throws Exception
    {
        assertThat(projectStore.listSecrets(projectId, SecretScopes.PROJECT), is(empty()));
        assertThat(projectStore.getSecretIfExists(projectId, KEY1, SecretScopes.PROJECT), is(Optional.absent()));
        assertThat(secretStore.getSecret(projectId, KEY1, SecretScopes.PROJECT), is(Optional.absent()));
        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.deleteSecretIfExists(projectId, KEY1, SecretScopes.PROJECT);
            return true;
        });
    }

    @Test
    public void singleSecret()
            throws Exception
    {
        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.putSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1);
            return true;
        });
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
    }

    @Test
    public void multipleSecrets()
            throws Exception
    {
        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.putSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1);
            store.putSecret(projectId, SecretScopes.PROJECT, KEY2, VALUE2);
            store.putSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2, VALUE3);
            return true;
        });

        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        // Delete with different scope should not delete the secret
        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.deleteSecretIfExists(projectId, SecretScopes.PROJECT_DEFAULT, KEY1);
            return true;
        });
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.deleteSecretIfExists(projectId, SecretScopes.PROJECT, KEY1);
            return true;
        });
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.absent()));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.deleteSecretIfExists(projectId, SecretScopes.PROJECT, KEY2);
            return true;
        });
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.absent()));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));
    }

    @Test
    public void getSecretWithScope()
            throws Exception
    {
        String[] scopes = {
                SecretScopes.PROJECT,
                SecretScopes.PROJECT_DEFAULT,
                "foobar"};

        for (String setScope : scopes) {
            projectStore.lockProjectById(projectId, (store, storedProject) -> {
                store.putSecret(projectId, setScope, KEY1, VALUE1);
                return true;
            });
            assertThat(secretStore.getSecret(projectId, setScope, KEY1), is(Optional.of(VALUE1)));

            for (String getScope : scopes) {
                if (getScope.equals(setScope)) {
                    continue;
                }
                assertThat("set: " + setScope + ", get: " + getScope, secretStore.getSecret(projectId, getScope, KEY1), is(Optional.absent()));
            }

            projectStore.lockProjectById(projectId, (store, storedProject) -> {
                store.deleteSecretIfExists(storedProject.getId(), setScope, KEY1);
                return true;
            });
        }
    }

    @Test
    public void concurrentPutShouldNotThrowExceptions()
            throws Exception
    {
		ExecutorService threads = Executors.newCachedThreadPool();
		ImmutableList.Builder<Future> futures = ImmutableList.builder();

		for (int i = 0; i < 10; i++) {
            String value = "thread-" + i;
			futures.add(threads.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        projectStore.lockProjectById(projectId, (store, storedProject) -> {
                            store.deleteSecretIfExists(projectId, SecretScopes.PROJECT, KEY1);
                            store.putSecret(projectId, SecretScopes.PROJECT, KEY1, value);
                            return true;
                        });
                    }
                }
                catch (Exception ex) {
                    throw Throwables.propagate(ex);
                }
			}));
		}

		for (Future f : futures.build()) {
			f.get();
		}
    }

    @Test
    public void rollbackTransaction()
            throws Exception
    {
        projectStore.lockProjectById(projectId, (store, storedProject) -> {
            store.putSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1);
            store.putSecret(projectId, SecretScopes.PROJECT, KEY2, VALUE1);
            store.putSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY1, VALUE1);
            return true;
        });

        try {
            projectStore.lockProjectById(projectId, (store, storedProject) -> {
                store.putSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE2);  // overwrite
                store.deleteSecretIfExists(projectId, SecretScopes.PROJECT, KEY2);  // delete
                store.putSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY1, VALUE2);  // overwrite
                store.putSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2, VALUE2);  // new key
                throw new RuntimeException("rollback");
            });
        }
        catch (RuntimeException expected) {
        }

        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.absent()));
    }
}

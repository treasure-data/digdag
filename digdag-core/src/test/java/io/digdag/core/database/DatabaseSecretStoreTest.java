package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.StoredProject;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DatabaseSecretStoreTest
{
    private static final String SECRET = Base64.getEncoder().encodeToString(
            Strings.repeat(".", 16).getBytes(UTF_8));

    private static final int SITE_ID = 17;

    private static final String KEY1 = "foo1";
    private static final String KEY2 = "foo2";
    private static final String VALUE1 = "bar1";
    private static final String VALUE2 = "bar2";
    private static final String VALUE3 = "bar3";

    private DatabaseFactory factory;
    private StoredProject storedProject;
    private DatabaseSecretControlStoreManager controlStoreManager;
    private DatabaseSecretStoreManager storeManager;
    private SecretControlStore secretControlStore;
    private SecretStore secretStore;
    private int projectId;

    @Before
    public void setUp()
            throws Exception
    {
        factory = DatabaseTestingUtils.setupDatabase();

        factory.begin(() -> {
            ProjectStore projectStore = factory.getProjectStoreManager().getProjectStore(SITE_ID);
            Project project = Project.of("proj1");
            storedProject = projectStore.putAndLockProject(project, (store, stored) -> stored);

            controlStoreManager = factory.getSecretControlStoreManager(SECRET);
            storeManager = factory.getSecretStoreManager(SECRET);
            secretControlStore = controlStoreManager.getSecretControlStore(SITE_ID);
            secretStore = storeManager.getSecretStore(SITE_ID);
            projectId = storedProject.getId();
            return null;
        });
    }

    @Test
    public void noSecret()
            throws Exception
    {
        assertThat(factory.autoCommit(() -> secretControlStore.listProjectSecrets(projectId, SecretScopes.PROJECT)), is(empty()));
        assertThat(secretStore.getSecret(projectId, KEY1, SecretScopes.PROJECT), is(Optional.absent()));
        factory.autoCommit(() -> secretControlStore.deleteProjectSecret(projectId, KEY1, SecretScopes.PROJECT));
    }

    @Test
    public void singleSecret()
            throws Exception
    {
        factory.autoCommit(() -> secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
    }

    @Test
    public void lockSecretGetsValue()
            throws Exception
    {
        Optional<String> absent = factory.begin(() -> secretControlStore.lockProjectSecret(projectId, SecretScopes.PROJECT, KEY1, (control, value) -> value));
        assertThat(absent, is(Optional.absent()));

        factory.begin(() -> secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1));
        Optional<String> present = factory.begin(() -> secretControlStore.lockProjectSecret(projectId, SecretScopes.PROJECT, KEY1, (control, value) -> value));
        assertThat(present, is(Optional.of(VALUE1)));
    }

    @Test
    public void lockSecret()
            throws Exception
    {
        factory.begin(() -> secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1));

        AtomicReference<Optional<String>> blockedGetValue = new AtomicReference<>(null);
        AtomicBoolean committing = new AtomicBoolean(false);
        AtomicBoolean blockedCheckComitting = new AtomicBoolean(false);

        Thread thread = factory.begin(() -> secretControlStore.lockProjectSecret(projectId, SecretScopes.PROJECT, KEY1, (control, value) -> {
            try {
                AtomicBoolean started = new AtomicBoolean(false);

                Thread t = new Thread(() -> {
                    started.set(true);
                    try {
                        blockedGetValue.set(
                                factory.begin(() -> secretControlStore.lockProjectSecret(projectId, SecretScopes.PROJECT, KEY1, (c1, v1) -> v1)));
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    blockedCheckComitting.set(committing.get());
                });
                t.setDaemon(true);
                t.start();

                // wait until the thread starts
                while (!started.get()) {
                    Thread.sleep(500);
                }
                // wait some more to make sure that lockProjectSecret is called & blocked
                Thread.sleep(500);

                control.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1 + ".changed");

                committing.set(true);
                return t;
            }
            catch (InterruptedException ex) {
                throw new AssertionError(ex);
            }
        }));

        thread.join();

        assertThat(blockedGetValue.get(), is(Optional.of(VALUE1 + ".changed")));
        assertThat(blockedCheckComitting.get(), is(true));
    }

    @Test
    public void multipleSecrets()
            throws Exception
    {
        factory.begin(() -> {
                    secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, VALUE1);
                    secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY2, VALUE2);
                    secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2, VALUE3);
                    return null;
                });

        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        // Delete with different scope should not delete the secret
        factory.autoCommit(() -> secretControlStore.deleteProjectSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY1));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.of(VALUE1)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        factory.autoCommit(() -> secretControlStore.deleteProjectSecret(projectId, SecretScopes.PROJECT, KEY1));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY1), is(Optional.absent()));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT, KEY2), is(Optional.of(VALUE2)));
        assertThat(secretStore.getSecret(projectId, SecretScopes.PROJECT_DEFAULT, KEY2), is(Optional.of(VALUE3)));

        factory.autoCommit(() -> secretControlStore.deleteProjectSecret(projectId, SecretScopes.PROJECT, KEY2));
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
            factory.autoCommit(() -> secretControlStore.setProjectSecret(projectId, setScope, KEY1, VALUE1));
            assertThat(secretStore.getSecret(projectId, setScope, KEY1), is(Optional.of(VALUE1)));

            for (String getScope : scopes) {
                if (getScope.equals(setScope)) {
                    continue;
                }
                assertThat("set: " + setScope + ", get: " + getScope, secretStore.getSecret(projectId, getScope, KEY1), is(Optional.absent()));
            }

            factory.autoCommit(() -> secretControlStore.deleteProjectSecret(storedProject.getId(), setScope, KEY1));
        }
    }


    @Test
    public void concurrentPutShouldNotThrowExceptions()
            throws Exception
    {
        ExecutorService threads = Executors.newCachedThreadPool();
        ImmutableList.Builder<Future> futures = ImmutableList.builder();

        for (int i = 0; i < 20; i++) {
            String value = "thread-" + i;
            futures.add(threads.submit(() -> {
                try {
                    factory.begin(() -> {
                        for (int j = 0; j < 50; j++) {
                            secretControlStore.deleteProjectSecret(projectId, SecretScopes.PROJECT, KEY1);
                            secretControlStore.setProjectSecret(projectId, SecretScopes.PROJECT, KEY1, value);
                        }
                        return null;
                    });
                }
                catch (Exception e) {
                    throw ThrowablesUtil.propagate(e);
                }
            }));
        }

        for (Future f : futures.build()) {
            f.get();
        }
    }
}

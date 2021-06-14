package io.digdag.core.agent;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.DatabaseFactory;
import io.digdag.core.database.DatabaseTestingUtils;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.digdag.core.database.DatabaseTestingUtils.createConfig;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class DefaultSecretProviderTest
{
    private static final YamlConfigLoader YAML_CONFIG_LOADER = new YamlConfigLoader();
    private static final ConfigFactory CONFIG_FACTORY = createConfigFactory();

    private static final int SITE_ID = 1;
    private static final int PROJECT_ID = 2;

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Mock SecretStore secretStore;

    private final SecretAccessContext secretAccessContext = SecretAccessContext.builder()
            .siteId(SITE_ID)
            .projectId(PROJECT_ID)
            .revision("foo")
            .workflowName("bar")
            .operatorType("baz")
            .taskName("quux")
            .build();
    private DatabaseFactory databaseFactory;

    @Before
    public void setUp()
            throws Exception
    {
        MockitoAnnotations.initMocks(this);
        databaseFactory = DatabaseTestingUtils.setupDatabase();
    }

    @Test
    public void testDefaultAccessibleSecret()
            throws Exception
    {
        String expectedSecret = "foo-secret";
        String key = "foo";
        Config mounts = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.of(expectedSecret));

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, mounts, secretStore);

        String secret = provider.getSecret(key);

        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, key);

        assertThat(secret, is(expectedSecret));
    }

    @Test
    @Parameters({
            "foo        | foo: true                  | foo",
            "foo        | foo: bar                   | bar",
            "foo.secret | foo: true                  | foo.secret",
            "foo.secret | foo: bar                   | bar.secret",
            "foo.a.b    | foo: {a: true}             | foo.a.b",
            "foo.a.b    | foo: {a: bar.a\\, b: quux} | bar.a.b"
    })
    public void testUserGrantedSecret(String key, String mountsYaml, String expectedKey)
            throws Exception
    {
        String expectedSecret = "the-secret";

        Config mounts = YAML_CONFIG_LOADER.loadString(mountsYaml).toConfig(CONFIG_FACTORY);

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, expectedKey)).thenReturn(Optional.of(expectedSecret));

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, mounts, secretStore);

        String secret = provider.getSecret(key);

        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, expectedKey);

        assertThat(secret, is(expectedSecret));
    }

    @Test
    public void verifyProjectScopePrecedence()
            throws Exception
    {
        String projectSecret = "project-secret";
        String projectDefaultSecret = "project-default-secret";
        String key = "foo";
        Config mounts = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.of(projectSecret));
        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key)).thenReturn(Optional.of(projectDefaultSecret));

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, mounts, secretStore);

        String secret = provider.getSecret(key);

        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, key);
        verify(secretStore, never()).getSecret(PROJECT_ID, key, SecretScopes.PROJECT_DEFAULT);

        assertThat(secret, is(projectSecret));
    }

    @Test
    public void verifyProjectDefaultScopeFallback()
            throws Exception
    {
        String projectDefaultSecret = "project-default-secret";
        String key = "foo";
        Config mounts = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.absent());
        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key)).thenReturn(Optional.of(projectDefaultSecret));

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, mounts, secretStore);

        String secret = provider.getSecret(key);

        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, key);
        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key);

        assertThat(secret, is(projectDefaultSecret));
    }
}

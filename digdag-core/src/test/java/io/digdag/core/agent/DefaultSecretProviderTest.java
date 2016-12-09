package io.digdag.core.agent;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretAccessDeniedException;
import io.digdag.spi.SecretAccessPolicy;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import java.util.function.Predicate;
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class DefaultSecretProviderTest
{
    private static final YamlConfigLoader YAML_CONFIG_LOADER = new YamlConfigLoader();
    private static final ConfigFactory CONFIG_FACTORY = createConfigFactory();

    private static final int SITE_ID = 1;
    private static final int PROJECT_ID = 2;

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Mock SecretAccessPolicy secretAccessPolicy;
    @Mock SecretStore secretStore;
    @Mock Predicate<String> operatorSecretFilter;

    private final SecretAccessContext secretAccessContext = SecretAccessContext.builder()
            .siteId(SITE_ID)
            .projectId(PROJECT_ID)
            .revision("foo")
            .workflowName("bar")
            .operatorType("baz")
            .taskName("quux")
            .build();

    @Before
    public void setUp()
            throws Exception
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void verifyUndeclaredAccessIsDenied()
            throws Exception
    {
        // system acl doesn't allow.
        // user doesn't grant explicitly.
        // operator accepts.
        // -> reject (no permission by system or user)

        String key = "foo";
        Config grants = createConfig();

        when(secretAccessPolicy.isSecretAccessible(secretAccessContext, key)).thenReturn(false);
        when(operatorSecretFilter.test(key)).thenReturn(true);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        try {
            provider.getSecret(key);
            fail("Expected " + SecretAccessDeniedException.class.getName());
        }
        catch (SecretAccessDeniedException e) {
            assertThat(e.getKey(), is(key));
        }

        verify(secretAccessPolicy).isSecretAccessible(secretAccessContext, key);
        verify(operatorSecretFilter).test(key);

        verifyNoMoreInteractions(secretStore);
    }

    @Test
    public void testDefaultAccessibleSecret()
            throws Exception
    {
        // system acl allows.
        // user doesn't grant explicitly.
        // operator accepts.
        // -> allow (system-default trusted access)

        String expectedSecret = "foo-secret";
        String key = "foo";
        Config grants = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.of(expectedSecret));
        when(secretAccessPolicy.isSecretAccessible(secretAccessContext, key)).thenReturn(true);
        when(operatorSecretFilter.test(key)).thenReturn(true);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        String secret = provider.getSecret(key);

        verify(secretAccessPolicy).isSecretAccessible(secretAccessContext, key);
        verify(operatorSecretFilter).test(key);
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
    public void testUserGrantedSecret(String key, String grantsYaml, String expectedKey)
            throws Exception
    {
        // system acl doesn't allow.
        // user grants explicitly.
        // operator accepts.
        // -> allow (user-granted access)

        String expectedSecret = "the-secret";

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, expectedKey)).thenReturn(Optional.of(expectedSecret));
        when(secretAccessPolicy.isSecretAccessible(any(SecretAccessContext.class), anyString())).thenReturn(false);
        when(operatorSecretFilter.test(key)).thenReturn(true);

        Config grants = YAML_CONFIG_LOADER.loadString(grantsYaml).toConfig(CONFIG_FACTORY);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        String secret = provider.getSecret(key);

        verifyNoMoreInteractions(secretAccessPolicy);
        verify(operatorSecretFilter).test(key);
        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, expectedKey);

        assertThat(secret, is(expectedSecret));
    }

    @Test
    public void testOperatorFilteredSecret()
            throws Exception
    {
        // system acl allows.
        // user doesn't allow explicitly.
        // operator doesn't accept.
        // -> reject (operator-filtered access)

        String key = "foo";
        Config grants = createConfig();

        when(operatorSecretFilter.test(key)).thenReturn(false);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        try {
            provider.getSecret(key);
            fail("Expected " + SecretAccessFilteredException.class.getName());
        }
        catch (SecretAccessFilteredException e) {
            assertThat(e.getKey(), is(key));
        }

        verifyNoMoreInteractions(secretAccessPolicy);

        verify(operatorSecretFilter).test(key);
        verifyNoMoreInteractions(secretStore);
    }

    @Test
    public void verifyProjectScopePrecedence()
            throws Exception
    {
        String projectSecret = "project-secret";
        String projectDefaultSecret = "project-default-secret";
        String key = "foo";
        Config grants = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.of(projectSecret));
        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key)).thenReturn(Optional.of(projectDefaultSecret));
        when(secretAccessPolicy.isSecretAccessible(secretAccessContext, key)).thenReturn(true);
        when(operatorSecretFilter.test(key)).thenReturn(true);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        String secret = provider.getSecret(key);

        verify(operatorSecretFilter).test(key);
        verify(secretAccessPolicy).isSecretAccessible(secretAccessContext, key);
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
        Config grants = createConfig();

        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT, key)).thenReturn(Optional.absent());
        when(secretStore.getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key)).thenReturn(Optional.of(projectDefaultSecret));
        when(secretAccessPolicy.isSecretAccessible(secretAccessContext, key)).thenReturn(true);
        when(operatorSecretFilter.test(key)).thenReturn(true);

        DefaultSecretProvider provider = new DefaultSecretProvider(secretAccessContext, secretAccessPolicy, grants, operatorSecretFilter, secretStore);

        String secret = provider.getSecret(key);

        verify(operatorSecretFilter).test(key);
        verify(secretAccessPolicy).isSecretAccessible(secretAccessContext, key);
        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT, key);
        verify(secretStore).getSecret(PROJECT_ID, SecretScopes.PROJECT_DEFAULT, key);

        assertThat(secret, is(projectDefaultSecret));
    }
}

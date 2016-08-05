package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClientBuilder;
import com.treasuredata.client.TDClientConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Map;

import static io.digdag.client.DigdagClient.objectMapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TDOperatorTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final ConfigFactory configFactory = new ConfigFactory(objectMapper());

    private Config newConfig()
    {
        return configFactory.create();
    }

    @Test
    public void verifyEmptyDatabaseParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
            .set("database", "")
            .set("apikey", "foobar");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyWhitespaceDatabaseParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
            .set("database", " \t\n")
            .set("apikey", "foobar");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyEmptyApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
            .set("database", "foobar")
            .set("apikey", "");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyWhitespaceApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
            .set("database", "foobar")
            .set("apikey", " \n\t");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void testFromConfig()
            throws Exception
    {
        Config config = newConfig()
            .set("database", "foobar")
            .set("apikey", "quux");
        TDOperator.fromConfig(config);
    }

    @Test
    public void testProxyConfig()
    {
        Config config = newConfig()
            .set("apikey", "foobar")
            .set("proxy",
                    newConfig()
                        .set("enabled", "true")
                        .set("host", "example.com")
                        .set("port", "9119")
                        .set("user", "me")
                        .set("password", "'(#%")
                        .set("use_ssl", true));

        TDClientBuilder builder = TDClientFactory.clientBuilderFromConfig(config);
        TDClientConfig clientConfig = builder.buildConfig();

        assertThat(clientConfig.proxy.get().getUser(), is(Optional.of("me")));
        assertThat(clientConfig.proxy.get().getPassword(), is(Optional.of("'(#%")));
        assertThat(clientConfig.proxy.get().getUri(), is(URI.create("https://example.com:9119")));
    }
}

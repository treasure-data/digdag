package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class TDOperatorTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final ObjectMapper mapper = new ObjectMapper();
    private final YamlConfigLoader loader = new YamlConfigLoader();
    private final ConfigFactory configFactory = new ConfigFactory(mapper);

    @Test
    public void verifyEmptyDatabaseParameterIsRejected()
            throws Exception
    {
        Map<String, String> configInput = ImmutableMap.of(
                "database", "",
                "apikey", "foobar");

        Config config = loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyWhitespaceDatabaseParameterIsRejected()
            throws Exception
    {
        Map<String, String> configInput = ImmutableMap.of(
                "database", " \t\n",
                "apikey", "foobar");

        Config config = loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyEmptyApiKeyParameterIsRejected()
            throws Exception
    {
        Map<String, String> configInput = ImmutableMap.of(
                "database", "foobar",
                "apikey", "");

        Config config = loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void verifyWhitespaceApiKeyParameterIsRejected()
            throws Exception
    {
        Map<String, String> configInput = ImmutableMap.of(
                "database", "foobar",
                "apikey", " \n\t");

        Config config = loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(config);
    }

    @Test
    public void testFromConfig()
            throws Exception
    {
        Map<String, String> configInput = ImmutableMap.of(
                "database", "foobar",
                "apikey", "quux");

        Config config = loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);
        TDOperator.fromConfig(config);
    }
}

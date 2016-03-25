package io.digdag.core.config;

import com.google.common.io.Resources;
import io.digdag.client.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class YamlConfigLoaderTest
{

    YamlConfigLoader loader;

    @Before
    public void setUp()
            throws Exception
    {
        loader = new YamlConfigLoader();
    }

    @Test(expected = ConfigException.class)
    public void verifyDuplicateKeysDisallowed()
            throws Exception
    {
        loader.loadString("{\"a\":1, \"a\":2}");
    }
}
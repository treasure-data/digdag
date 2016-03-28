package io.digdag.core.config;

import java.nio.file.Files;
import java.nio.file.Path;
import com.google.common.io.Resources;
import org.yaml.snakeyaml.error.YAMLException;
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

    @Test(expected = YAMLException.class)
    public void verifyDuplicateKeysDisallowed()
            throws Exception
    {
        loader.loadString("{\"a\":1, \"a\":2}");
    }

    @Test(expected = YAMLException.class)
    public void verifyDuplicateKeysDisallowedWithParameterizedLoad()
            throws Exception
    {
        Path temp = Files.createTempFile("digdag-YamlConfigLoaderTest", ".yml");
        Files.write(temp, "{\"a\":1, \"a\":2}".getBytes(UTF_8));
        loader.loadParameterizedFile(temp.toFile(), null);
    }
}

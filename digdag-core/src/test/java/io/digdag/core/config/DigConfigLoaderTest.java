package io.digdag.core.config;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Ignore;
import org.yaml.snakeyaml.error.YAMLException;
import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DigConfigLoaderTest
{
    DigConfigLoader loader;

    @Before
    public void setUp()
            throws Exception
    {
        loader = new DigConfigLoader();
    }

    @Ignore("FIXME: hjson parser does not implement this")
    @Test(expected = YAMLException.class)
    public void verifyDuplicateKeysDisallowed()
            throws Exception
    {
        loader.loadString("{\"a\":1, \"a\":2}");
    }

    @Ignore("FIXME: hjson parser does not implement this")
    @Test(expected = YAMLException.class)
    public void verifyDuplicateKeysDisallowedWithParameterizedLoad()
            throws Exception
    {
        Path temp = Files.createTempFile("digdag-DigConfigLoaderTest", ".dig");
        Files.write(temp, "{\"a\":1, \"a\":2}".getBytes(UTF_8));
        loader.loadParameterizedFile(temp.toFile(), null);
    }
}

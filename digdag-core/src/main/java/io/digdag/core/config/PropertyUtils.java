package io.digdag.core.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;

public class PropertyUtils
{
    private PropertyUtils()
    { }

    public static Properties loadFile(Path file)
        throws IOException
    {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        }
        return props;
    }

    public static ConfigElement toConfigElement(Properties props)
    {
        Config builder = new ConfigFactory(new ObjectMapper()).create();
        for (String key : props.stringPropertyNames()) {
            builder.set(key, props.getProperty(key));
        }
        return ConfigElement.copyOf(builder);
    }

    public static Map<String, String> toMap(Properties props, String prefix)
    {
        Map<String, String> map = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                map.put(key.substring(prefix.length()), props.getProperty(key));
            }
        }
        return map;
    }
}

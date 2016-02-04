package io.digdag.core.config;

import java.util.Properties;
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

    public static Properties loadFile(File file)
        throws IOException
    {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
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
}

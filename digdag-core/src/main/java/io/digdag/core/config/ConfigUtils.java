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

public class ConfigUtils
{
    private ConfigUtils()
    { }

    public static com.typesafe.config.Config loadFile(File file)
        throws IOException
    {
        com.typesafe.config.Config raw = com.typesafe.config.ConfigFactory.parseFile(file);
        return com.typesafe.config.ConfigFactory.load(raw);
    }

    public static ConfigElement toConfigElement(com.typesafe.config.Config props)
    {
        Config builder = new ConfigFactory(new ObjectMapper()).create();
        for (String key : props.stringPropertyNames()) {
            builder.set(key, props.getProperty(key));
        }
        return ConfigElement.copyOf(builder);
    }
}

package io.digdag.cli;

import java.nio.file.Path;
import java.nio.file.Paths;

class ConfigUtil
{
    static Path defaultConfigPath()
    {
        return configHome().resolve("digdag").resolve("config");
    }

    private static Path configHome()
    {
        String configHome = System.getenv("XDG_CONFIG_HOME");
        if (configHome != null) {
            return Paths.get(configHome);
        }
        return Paths.get(System.getProperty("user.home"), ".config");
    }
}

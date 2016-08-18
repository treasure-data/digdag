package io.digdag.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

class ConfigUtil
{
    static Path defaultConfigPath(Map<String, String> env)
    {
        return configHome(env).resolve("digdag").resolve("config");
    }

    static Path defaultLocalPluginPath(Map<String, String> env)
    {
        return configHome(env).resolve("digdag").resolve("plugins");
    }

    private static Path configHome(Map<String, String> env)
    {
        String configHome = env.get("XDG_CONFIG_HOME");
        if (configHome != null) {
            return Paths.get(configHome);
        }
        return Paths.get(System.getProperty("user.home"), ".config");
    }
}

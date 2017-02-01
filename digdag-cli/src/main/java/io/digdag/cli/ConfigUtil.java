package io.digdag.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ConfigUtil
{
    public static Path defaultConfigPath(Map<String, String> env)
    {
        return digdagConfigHome(env).resolve("config");
    }

    public static Path digdagConfigHome(Map<String, String> env)
    {
        String digdagConfigHomeEnv = env.get("DIGDAG_CONFIG_HOME");
        Path digdagConfigHome;
        if (digdagConfigHomeEnv != null) {
            digdagConfigHome = Paths.get(digdagConfigHomeEnv);
        }
        else {
            digdagConfigHome = configHome(env).resolve("digdag");
        }
        return digdagConfigHome;
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

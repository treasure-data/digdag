package io.digdag.util;

import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import static java.util.Objects.requireNonNull;

public class ConfigContext
{
    public static ConfigContext of(OperatorContext context)
    {
        return new ConfigContext(
                context.getTaskRequest().getLocalConfig(),
                context.getTaskRequest().getConfig(),
                context.getSecrets());
    }

    private final Config localConfig;
    private final Config runtimeParams;
    private final SecretProvider secrets;

    private ConfigContext(Config localConfig, Config runtimeParams, SecretProvider secrets)
    {
        this.localConfig = requireNonNull(localConfig, "localConfig");
        this.runtimeParams = requireNonNull(runtimeParams, "runtimeParams");
        this.secrets = requireNonNull(secrets, "secrets");
    }

    public ConfigScope configScope(ConfigSelector selector)
    {
        return configScope(selector, selector.getPrimaryScope());
    }

    public ConfigScope configScope(ConfigSelector configSelector, String scope)
    {
        return new ConfigScope(scope, localConfig, runtimeParams, configSelector, secrets);
    }
}

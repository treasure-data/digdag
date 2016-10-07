package io.digdag.core.session;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;
import java.util.List;

public class ParameterUpdate
{
    private final List<ConfigKey> resetKeys;
    private final Config mergeParams;

    public ParameterUpdate(List<ConfigKey> resetKeys, Config mergeParams)
    {
        this.resetKeys = resetKeys;
        this.mergeParams = mergeParams;
    }

    public void applyTo(Config config)
    {
        for (ConfigKey resetKey : resetKeys) {
            removeConfigKey(config, resetKey);
        }
        config.merge(mergeParams);
    }

    private static void removeConfigKey(Config config, ConfigKey key)
    {
        for (String nestName : key.getNestNames()) {
            Optional<Config> nest = config.getOptionalNested(nestName);
            if (!nest.isPresent()) {
                return;
            }
            config = nest.get();
        }
        config.remove(key.getLastName());
    }
}

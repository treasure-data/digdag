package io.digdag.core.session;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigPath;
import java.util.List;

public class ParameterUpdate
{
    private final List<ConfigPath> resetPaths;
    private final Config mergeParams;

    public ParameterUpdate(List<ConfigPath> resetPaths, Config mergeParams)
    {
        this.resetPaths = resetPaths;
        this.mergeParams = mergeParams;
    }

    public void applyTo(Config config)
    {
        for (ConfigPath resetPath : resetPaths) {
            removeConfigPath(config, resetPath);
        }
        config.merge(mergeParams);
    }

    private static void removeConfigPath(Config config, ConfigPath path)
    {
        for (String nestName : path.getNestNames()) {
            Optional<Config> nest = config.getOptionalNested(nestName);
            if (!nest.isPresent()) {
                return;
            }
            config = nest.get();
        }
        config.remove(path.getLastName());
    }
}

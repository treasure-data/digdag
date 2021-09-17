package io.digdag.core.acroute;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.AccountRoutingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.digdag.spi.AccountRouting.ModuleType;

public class DefaultAccountRoutingFactory implements AccountRoutingFactory
{
    private final Config systemConfig;
    private static Logger logger = LoggerFactory.getLogger(DefaultAccountRoutingFactory.class);

    @Inject
    public DefaultAccountRoutingFactory(Config systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    @Override
    public String getType()
    {
        return "default";
    }

    @Override
    public AccountRouting newAccountRouting(ModuleType module)
    {

        return fromConfig(systemConfig, Optional.of(module.toString()));
    }

    private static List<Integer> parseIdList(String s)
    {
        return Arrays.stream(s.split(","))
                .filter(x -> !x.equals(""))
                .map(Integer::valueOf)
                .collect(Collectors.toList());
    }

    public static DefaultAccountRouting fromConfig(Config config, Optional<String> prefixKey)
    {
        String prefix = prefixKey.transform( x -> x + ".").or("") + "account_routing.";
        Boolean enabled = config.get(prefix + "enabled", Boolean.class, false);
        List<Integer> include = parseIdList(config.get(prefix + "include", String.class, ""));
        List<Integer> exclude = parseIdList(config.get(prefix + "exclude", String.class, ""));

        // validation
        if (enabled) {
            if (include.isEmpty() && exclude.isEmpty()) {
                throw new ConfigException("account_routing: include or exclude must be defined exclusively.");
            }
            else if (enabled && !include.isEmpty() && !exclude.isEmpty()) {
                throw new ConfigException("account_routing: include or exclude must be defined exclusively.");
            }
            return new DefaultAccountRouting(enabled, include, exclude);
        }
        else {
            return new DefaultAccountRouting();
        }
    }
}

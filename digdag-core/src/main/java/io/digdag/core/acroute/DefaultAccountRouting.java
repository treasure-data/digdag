package io.digdag.core.acroute;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.AccountRouting;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultAccountRouting implements AccountRouting
{
    private final Boolean enabled;
    private final List<Integer> includeSiteIds;
    private final List<Integer> excludeSiteIds;

    @Override
    public Boolean enabled()
    {
        return enabled;
    }

    @Override
    public String getFilterSQL(String column) {
        if (enabled) {
            if (! includeSiteIds.isEmpty()) {
                return String.format("%s in (%s)",
                        column,
                        includeSiteIds.stream().map(i -> i.toString()).collect(Collectors.joining(",")));
            }
            else {
                return String.format("%s not in (%s)",
                        column,
                        excludeSiteIds.stream().map(i -> i.toString()).collect(Collectors.joining(",")));
            }
        }
        else {
            return "true";
        }
    }

    /**
     *
     * @param config system configuration
     * @param prefixKey prefix key to load. (e.g. "executor", "agent")
     * @return
     */
    public static DefaultAccountRouting of(Config config, Optional<String> prefixKey)
    {
        Optional<Config> cf = prefixKey.isPresent() ? config.getOptionalNested(prefixKey.get() + ".account_routing") : Optional.of(config);
        if (cf.isPresent()) {
            Boolean enabled = cf.get().get("enabled", Boolean.class, false);
            List<Integer> includes = cf.get().getListOrEmpty("include", Integer.class);
            List<Integer> excludes = cf.get().getListOrEmpty("exclude", Integer.class);

            // validation
            if (enabled) {
                if (includes.isEmpty() && excludes.isEmpty()) {
                    throw new ConfigException("account_routing: include or exclude must be defined exclusively.");
                }
                else if (enabled && !includes.isEmpty() && !excludes.isEmpty()) {
                    throw new ConfigException("account_routing: include or exclude must be defined exclusively.");
                }
            }
            return new DefaultAccountRouting(enabled, includes, excludes);
        }
        else {
            return new DefaultAccountRouting();
        }
    }

    public DefaultAccountRouting(Boolean enabled, List<Integer> includeSiteId, List<Integer> excludeSiteId )
    {
        this.enabled = enabled;
        this.includeSiteIds = new ArrayList<>(includeSiteId);
        this.excludeSiteIds = new ArrayList<>(excludeSiteId);
    }

    public DefaultAccountRouting()
    {
        this.enabled = false;
        this.includeSiteIds = new ArrayList<>();
        this.excludeSiteIds = new ArrayList<>();
    }

    public List<Integer> getIncludeSiteIds()
    {
        return includeSiteIds;
    }

    public List<Integer> getExcludeSiteIds()
    {
        return excludeSiteIds;
    }
}

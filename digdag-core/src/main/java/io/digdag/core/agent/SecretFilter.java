package io.digdag.core.agent;

import com.google.common.collect.ImmutableList;
import io.digdag.spi.SecretSelector;

import java.util.List;

class SecretFilter
{
    private final List<SecretSelector> selectors;

    private SecretFilter(List<SecretSelector> selectors)
    {
        this.selectors = ImmutableList.copyOf(selectors);
    }

    boolean match(String key)
    {
        return selectors.stream().anyMatch(s -> s.match(key));
    }

    static SecretFilter of(List<SecretSelector> selectors)
    {
        return new SecretFilter(selectors);
    }
}

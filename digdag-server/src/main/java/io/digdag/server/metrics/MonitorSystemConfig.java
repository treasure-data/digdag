package io.digdag.server.metrics;

import com.google.common.base.Optional;
import io.digdag.spi.metrics.DigdagMetrics.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface MonitorSystemConfig
{
    boolean getMonitorSystemEnable();

    boolean getCategoryDefaultEnable();

    boolean getCategoryAgentEnable();

    boolean getCategoryApiEnable();

    boolean getCategoryDbEnable();

    boolean getCategoryExecutorEnable();

    default boolean enable(Category category)
    {
        return getMonitorSystemEnable() && getCategoryEnable(category);
    }

    default boolean getCategoryEnable(Category category)
    {
        switch (category) {
            case DEFAULT:
                return getCategoryDefaultEnable();
            case AGENT:
                return getCategoryAgentEnable();
            case API:
                return getCategoryApiEnable();
            case DB:
                return getCategoryDbEnable();
            case EXECUTOR:
                return getCategoryExecutorEnable();
            default:
                return false;
        }
    }

    static Map<Category, Boolean> getEnabledCategories(Optional<String> config)
    {
        List<Category> ALL = Arrays.asList(Category.class.getEnumConstants());
        Map<Category, Boolean> map = new HashMap<>();
        for (Category c: ALL) {
            map.put(c, false);
        }
        List<Category> enables = config
                .transform( (s) ->
                        Arrays.asList(s.split(","))
                            .stream()
                            .map( (x) -> x.trim())
                            .flatMap( (x) -> {
                                    List<Category> ret;
                                    if (x.compareToIgnoreCase("all") == 0) {
                                        ret = ALL;
                                    }
                                    else {
                                        ret = Arrays.asList(Category.fromString(x));
                                    }
                                    return ret.stream();
                                })
                                .distinct()
                                .collect(Collectors.toList())
                )
                .or(ALL);
        for (Category c: enables) {
            map.put(c, true);
        }
        return map;
    }

}

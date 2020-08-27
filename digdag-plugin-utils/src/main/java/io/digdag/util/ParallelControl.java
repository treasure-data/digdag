package io.digdag.util;

import com.fasterxml.jackson.databind.JsonNode;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class ParallelControl
{
    public static ParallelControl of(Config config)
    {
        return new ParallelControl(config);
    }

    private final boolean isParallel;
    private final int parallelLimit;

    private ParallelControl(Config config)
    {
        final JsonNode parallelNode = config.getInternalObjectNode().get("_parallel");
        if (parallelNode == null) { // not specified, default
            this.isParallel = false;
            this.parallelLimit = 0;
        }
        else if (parallelNode.isBoolean() || parallelNode.isTextual()) { // _parallel: true/false
            // If _parallel is specified in subtasks (e.g. loop operators with parallel),
            // a variable ${..} is available, if set as variable ${..}, the value become text so that needs to accept here
            this.isParallel = config.get("_parallel", boolean.class, false);
            this.parallelLimit = 0; // no limit
        }
        else if (parallelNode.isObject()) { // _parallel: {limit: N}
            Config parallel = config.getNested("_parallel");
            this.isParallel = true; // always true
            this.parallelLimit = parallel.get("limit", int.class);
        }
        else { // unknown format
            throw new ConfigException(String.format("Invalid _parallel format: %s", parallelNode.toString()));
        }
    }

    public boolean isParallel() { return isParallel; }
    public int getParallelLimit() { return parallelLimit; }
}

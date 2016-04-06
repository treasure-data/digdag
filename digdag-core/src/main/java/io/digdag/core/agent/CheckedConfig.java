package io.digdag.core.agent;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.client.config.Config;

public class CheckedConfig
    extends Config
{
    private static class HashSetWithAllFlag<T>
            extends HashSet<T>
    {
        private boolean all = false;

        public void setAll(boolean v)
        {
            this.all = v;
        }

        public boolean isAll()
        {
            return all;
        }
    }

    private final HashSetWithAllFlag<String> usedKeys;

    public CheckedConfig(Config config)
    {
        this(config, new HashSetWithAllFlag<>());
    }

    private CheckedConfig(Config config, HashSetWithAllFlag<String> usedKeys)
    {
        super(config);
        this.usedKeys = usedKeys;
    }

    public List<String> getUsedKeys()
    {
        List<String> keys = new ArrayList<>(usedKeys);
        Collections.sort(keys);
        return keys;
    }

    public boolean isAllUsed()
    {
        return usedKeys.isAll();
    }

    @Override
    public Config deepCopy()
    {
        return new CheckedConfig(this, usedKeys);
    }

    @Override
    public ObjectNode getInternalObjectNode()
    {
        this.usedKeys.setAll(true);
        return super.getInternalObjectNode();
    }

    @Override
    public Config remove(String key)
    {
        this.usedKeys.add(key);
        return super.remove(key);
    }

    @Override
    public boolean has(String key)
    {
        this.usedKeys.add(key);
        return super.has(key);
    }

    @Override
    protected JsonNode get(String key)
    {
        this.usedKeys.add(key);
        return super.get(key);
    }

    @Override
    protected void set(String key, JsonNode value)
    {
        this.usedKeys.add(key);
        super.set(key, value);
    }
}

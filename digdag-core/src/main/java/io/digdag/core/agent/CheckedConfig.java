package io.digdag.core.agent;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.digdag.client.config.Config;

class CheckedConfig
    extends Config
{
    static class UsedKeysSet
            extends HashSet<String>
    {
        private boolean all = false;

        public void setAllUsed(boolean v)
        {
            this.all = v;
        }

        public boolean isAllUsed()
        {
            return all;
        }
    }

    private final UsedKeysSet usedKeys;

    CheckedConfig(Config config, UsedKeysSet usedKeys)
    {
        super(config);
        this.usedKeys = usedKeys;
    }

    @Override
    public Config deepCopy()
    {
        return new CheckedConfig(this, usedKeys);
    }

    @Override
    public ObjectNode getInternalObjectNode()
    {
        this.usedKeys.setAllUsed(true);
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

    // getKeys doesn't set usedKeys.setAllUsed(true) because operator plugins
    // should call get(key) after getKeys().
}

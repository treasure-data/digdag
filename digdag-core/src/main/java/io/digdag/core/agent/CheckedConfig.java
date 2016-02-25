package io.digdag.core.agent;

import java.util.List;
import java.util.Collection;
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
    private final Set<String> unusedKeys;

    public CheckedConfig(Config config, Collection<? extends String> shouldBeUsedKeys)
    {
        this(config, new HashSet<>(shouldBeUsedKeys));
    }

    private CheckedConfig(Config config, Set<String> unusedKeys)
    {
        super(config);
        this.unusedKeys = unusedKeys;
    }

    public List<String> getUnusedKeys()
    {
        List<String> keys = new ArrayList<>(unusedKeys);
        Collections.sort(keys);
        return keys;
    }

    @Override
    public ObjectNode getInternalObjectNode()
    {
        unusedKeys.clear();
        return super.getInternalObjectNode();
    }

    @Override
    public Config remove(String key)
    {
        unusedKeys.remove(key);
        return super.remove(key);
    }

    @Override
    public List<String> getKeys()
    {
        unusedKeys.clear();
        return super.getKeys();
    }

    @Override
    public boolean has(String key)
    {
        unusedKeys.remove(key);
        return super.has(key);
    }

    @Override
    protected JsonNode get(String key)
    {
        unusedKeys.remove(key);
        return super.get(key);
    }

    @Override
    protected void set(String key, JsonNode value)
    {
        unusedKeys.remove(key);
        super.set(key, value);
    }

    @Override
    public Config deepCopy()
    {
        return new CheckedConfig(this, unusedKeys);
    }
}

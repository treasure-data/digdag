package io.digdag.core.storage;

import java.util.Set;
import java.util.Map;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.fasterxml.jackson.databind.JsonNode;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFactory;

public class StorageManager
{
    private Map<String, StorageFactory> registry;  // TODO this should be extracted to a class named StorageRegistry as like OperatorRegistry

    public StorageManager(Set<StorageFactory> factories)
    {
        ImmutableMap.Builder<String, StorageFactory> builder = ImmutableMap.builder();
        for (StorageFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.registry = builder.build();
    }

    public Storage create(Config systemConfig, String configKeyPrefix)
    {
        String type = systemConfig.get(configKeyPrefix + ".type", String.class);
        return create(type, systemConfig, configKeyPrefix);
    }

    public Storage create(String type, Config systemConfig, String configKeyPrefix)
    {
        Config config = extractKeyPrefix(systemConfig, configKeyPrefix + "." + type);
        StorageFactory factory = registry.get(type);
        if (factory == null) {
            throw new ConfigException("Unknown storage type: " + type);
        }
        return factory.newStorage(config);
    }

    public static Config extractKeyPrefix(Config config, String configKeyPrefix)
    {
        Config extracted = config.getFactory().create();
        for (String key : config.getKeys()) {
            if (key.startsWith(configKeyPrefix)) {
                extracted.set(
                        key.substring(configKeyPrefix.length()),
                        config.get(key, JsonNode.class).deepCopy());
            }
        }
        return extracted;
    }

    public static byte[] calculateMd5(byte[] data)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(data);
        }
        catch (NoSuchAlgorithmException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public static String encodeHexMd5(byte[] hexBin)
    {
        return BaseEncoding.base32Hex().encode(hexBin);
    }

    public static byte[] decodeHexMd5(String hexStr)
    {
        return BaseEncoding.base32Hex().decode(hexStr);
    }
}

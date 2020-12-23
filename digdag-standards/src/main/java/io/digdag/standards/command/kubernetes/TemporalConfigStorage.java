package io.digdag.standards.command.kubernetes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class TemporalConfigStorage
{
    private static final String TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX = "agent.command_executor.kubernetes.config_storage.";

    public static TemporalConfigStorage createByTarget(final StorageManager storageManager, final String target, final Config systemConfig)
    {
        Config validatedSystemConfig = validateSystemConfig(target, systemConfig);
        final Storage storage = storageManager.create(validatedSystemConfig, TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX + target + ".");
        return new TemporalConfigStorage(storage);
    }

    @VisibleForTesting
    static Config validateSystemConfig(String target, Config systemConfig)
    {
        String storageTypeKey = TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX + target + ".type";
        if (!systemConfig.has(storageTypeKey))
        {
            throw new ConfigException(String.format("kubernetes config_storage must have %s", storageTypeKey));
        }

        String storageType = systemConfig.get(storageTypeKey, String.class);
        String storageBucketKey = TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX + target + "." + storageType + ".bucket";
        if (!systemConfig.has(storageBucketKey))
        {
            throw new ConfigException(String.format("kubernetes config_storage must have %s", storageBucketKey));
        }
        return systemConfig;
    }

    private final Storage storage;

    private TemporalConfigStorage(final Storage storage)
    {
        this.storage = storage;
    }

    public void uploadFile(final String key, final Path filePath)
            throws IOException
    {
        final File file = filePath.toFile();
        storage.put(key, file.length(), () -> new FileInputStream(file));
    }

    public String getDirectDownloadUrl(final String key)
    {
        return storage.getDirectDownloadHandle(key).get().getUrl().toString();
    }

    public String getDirectUploadUrl(final String key)
    {
        return storage.getDirectUploadHandle(key).get().getUrl().toString();
    }

    public InputStream getContentInputStream(final String key)
    {
        try {
            return storage.open(key).getContentInputStream();
        }
        catch (StorageFileNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    public Long getDirectDownloadExpiration()
    {
        return storage.getDirectDownloadExpiration();
    }

    public Long getDirectUploadExpiration()
    {
        return storage.getDirectUploadExpiration();
    }
}

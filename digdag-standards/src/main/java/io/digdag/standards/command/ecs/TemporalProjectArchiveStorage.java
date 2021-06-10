package io.digdag.standards.command.ecs;

import io.digdag.client.config.Config;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class TemporalProjectArchiveStorage
{
    private static final String PARAMS_PREFIX = "agent.command_executor.ecs.temporal_storage.";

    public static TemporalProjectArchiveStorage of(final StorageManager sm, final Config systemConfig)
    {
        final Storage storage = sm.create(systemConfig, PARAMS_PREFIX);
        return new TemporalProjectArchiveStorage(storage);
    }

    private final Storage storage;

    private TemporalProjectArchiveStorage(final Storage storage)
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
            throw ThrowablesUtil.propagate(e);
        }
    }
}

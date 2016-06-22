package io.digdag.core.storage;

import java.io.InputStream;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.repository.ArchiveType;
import io.digdag.core.storage.StorageManager;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;
import static java.util.Locale.ENGLISH;

public class ArchiveStorage
{
    public interface Upload
    {
        byte[] get() throws InterruptedException;
    }

    public static class Location
    {
        private final ArchiveType archiveType;
        private final String path;

        public Location(ArchiveType archiveType, String path)
        {
            this.archiveType = archiveType;
            this.path = path;
        }

        public ArchiveType getArchiveType()
        {
            return archiveType;
        }

        public String getPath()
        {
            return path;
        }
    }

    private final StorageManager storageManager;
    private final ArchiveType uploadArchiveType;
    private final ExecutorService executor;
    private final Config systemConfig;

    public ArchiveStorage(StorageManager storageManager, Config systemConfig)
    {
        this.storageManager = storageManager;
        this.systemConfig = systemConfig;
        this.uploadArchiveType = systemConfig.get("archive.type", ArchiveType.class, ArchiveType.DB);
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("archive-upload-%d")
                .build());
    }

    private Storage getStorage(ArchiveType type)
    {
        return storageManager.create(type.getName(), systemConfig, "archive");
    }

    public Location newArchiveLocation(
            int siteId, String projectName, String revisionName,
            long contentLength)
    {
        if (uploadArchiveType.equals(ArchiveType.DB)) {
            return new Location(uploadArchiveType, null);
        }
        else {
            return new Location(uploadArchiveType, formatFilePath(siteId, projectName, revisionName));
        }
    }

    public Upload startUpload(InputStream in, long contentLength,
            final Location location)
    {
        final Future<String> future = executor.submit(() -> getStorage(location.getArchiveType()).put(location.getPath(), contentLength, in));
        return () -> {
                try {
                    return StorageManager.decodeHexMd5(future.get());
                }
                catch (ExecutionException ex) {
                    throw Throwables.propagate(ex);
                }
            };
    }

    public InputStream openArchive(ArchiveType type, String path)
        throws StorageFileNotFoundException
    {
        return getStorage(type).open(path).getContentInputStream();
    }

    private String formatFilePath(int siteId, String projectName, String revisionName)
    {
        return String.format(ENGLISH,
                "%d/%s/%s.tar.gz",
                siteId, projectName, revisionName);
    }
}

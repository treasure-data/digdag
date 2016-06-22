package io.digdag.core.storage;

import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.repository.ArchiveType;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.storage.StorageManager;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;
import static java.util.Locale.ENGLISH;
import static io.digdag.core.storage.StorageManager.decodeHex;

public class ArchiveManager
{
    public interface Upload
    {
        byte[] get() throws IOException, InterruptedException;
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

    @Inject
    public ArchiveManager(StorageManager storageManager, Config systemConfig)
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
        return storageManager.create(type.getName(), systemConfig, "archive.");
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

    private String formatFilePath(int siteId, String projectName, String revisionName)
    {
        return String.format(ENGLISH,
                "%d/%s/%s.tar.gz",
                siteId, projectName, revisionName);
    }

    public Upload startUpload(InputStream in, long contentLength,
            final Location location)
    {
        final Future<String> future = executor.submit(() -> {
            try {
                return getStorage(location.getArchiveType()).put(location.getPath(), contentLength, in);
            }
            finally {
                in.close();
            }
        });
        return () -> {
                try {
                    return decodeHex(future.get());
                }
                catch (ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    Throwables.propagateIfInstanceOf(cause, IOException.class);
                    throw Throwables.propagate(cause);
                }
            };
    }

    public Optional<InputStream> openArchive(ProjectStore ps, int projectId, String revisionName)
        throws ResourceNotFoundException, StorageFileNotFoundException
    {
        StoredRevision rev;
        if (revisionName == null) {
            rev = ps.getLatestRevision(projectId);
        }
        else {
            rev = ps.getRevisionByName(projectId, revisionName);
        }
        return openArchive(ps, rev);
    }

    private Optional<InputStream> openArchive(ProjectStore ps, StoredRevision rev)
        throws ResourceNotFoundException, StorageFileNotFoundException
    {
        ArchiveType type = rev.getArchiveType();
        if (type.equals(ArchiveType.NONE)) {
            return Optional.absent();
        }
        else if (type.equals(ArchiveType.DB)) {
            return Optional.of(new ByteArrayInputStream(ps.getRevisionArchiveData(rev.getId())));
        }
        else {
            return Optional.of(getStorage(type).open(rev.getArchivePath().or("")).getContentInputStream());
        }
    }
}

package io.digdag.core.storage;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObject;
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

    public interface StoredArchive
    {
        Optional<byte[]> getByteArray();

        Optional<DirectDownloadHandle> getDirectDownloadHandle();

        StorageObject open() throws StorageFileNotFoundException;
    }

    private final StorageManager storageManager;
    private final ArchiveType uploadArchiveType;
    private final Config systemConfig;

    @Inject
    public ArchiveManager(StorageManager storageManager, Config systemConfig)
    {
        this.storageManager = storageManager;
        this.systemConfig = systemConfig;
        this.uploadArchiveType = systemConfig.get("archive.type", ArchiveType.class, ArchiveType.DB);
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

    public Storage getStorage(ArchiveType type)
    {
        return storageManager.create(type.getName(), systemConfig, "archive.");
    }

    private static final DateTimeFormatter DATE_TIME_SUFFIX_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMM'T'ddHHmmss'Z'").withZone(ZoneId.of("UTC"));

    private String formatFilePath(int siteId, String projectName, String revisionName)
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(projectName), "projectName");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(revisionName), "revisionName");
        return String.format(ENGLISH,
                "%d/%s/%s.%s.tar.gz",
                siteId, projectName, revisionName, DATE_TIME_SUFFIX_FORMAT.format(Instant.now()));
    }

    public Optional<StorageObject> openArchive(ProjectStore ps, int projectId, String revisionName)
        throws ResourceNotFoundException, StorageFileNotFoundException
    {
        StoredRevision rev = findRevision(ps, projectId, revisionName);

        ArchiveType type = rev.getArchiveType();
        if (type.equals(ArchiveType.NONE)) {
            return Optional.absent();
        }
        else if (type.equals(ArchiveType.DB)) {
            byte[] data = ps.getRevisionArchiveData(rev.getId());
            return Optional.of(
                    new StorageObject(
                        new ByteArrayInputStream(data),
                        data.length)
                    );
        }
        else {
            return Optional.of(getStorage(type).open(rev.getArchivePath().or("")));
        }
    }

    public Optional<StoredArchive> getArchive(ProjectStore ps, int projectId, String revisionName)
        throws ResourceNotFoundException
    {
        StoredRevision rev = findRevision(ps, projectId, revisionName);

        ArchiveType type = rev.getArchiveType();
        if (type.equals(ArchiveType.NONE)) {
            return Optional.absent();
        }
        else if (type.equals(ArchiveType.DB)) {
            byte[] data = ps.getRevisionArchiveData(rev.getId());
            return Optional.of(new StoredArchive() {
                public Optional<byte[]> getByteArray()
                {
                    return Optional.of(data);
                }

                public Optional<DirectDownloadHandle> getDirectDownloadHandle()
                {
                    return Optional.absent();
                }

                public StorageObject open()
                {
                    return new StorageObject(new ByteArrayInputStream(data), data.length);
                }
            });
        }
        else {
            Storage storage = getStorage(type);
            return Optional.of(new StoredArchive() {
                public Optional<byte[]> getByteArray()
                {
                    return Optional.absent();
                }

                public Optional<DirectDownloadHandle> getDirectDownloadHandle()
                {
                    return storage.getDirectDownloadHandle(rev.getArchivePath().or(""));
                }

                public StorageObject open()
                    throws StorageFileNotFoundException
                {
                    return storage.open(rev.getArchivePath().or(""));
                }
            });
        }
    }

    private StoredRevision findRevision(ProjectStore ps, int projectId, String revisionName)
        throws ResourceNotFoundException
    {
        if (revisionName == null) {
            return ps.getLatestRevision(projectId);
        }
        else {
            return ps.getRevisionByName(projectId, revisionName);
        }
    }
}

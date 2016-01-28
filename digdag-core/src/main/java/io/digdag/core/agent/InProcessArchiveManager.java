package io.digdag.core.agent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitResult;
import com.google.inject.Inject;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.core.repository.RepositoryStore;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;

public class InProcessArchiveManager
    implements ArchiveManager
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RepositoryStoreManager rm;
    private final Path tempDir;

    @Inject
    public InProcessArchiveManager(RepositoryStoreManager rm)
    {
        this.rm = rm;
        try {
            // TODO make this path configurable
            this.tempDir = Files.createTempDirectory("temp");
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        logger.debug("Using {} for working extract path", tempDir);
    }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, WithArchiveAction<T> func)
            throws IOException
    {
        try {
            RepositoryStore rs = rm.getRepositoryStore(request.getTaskInfo().getSiteId());
            StoredRevision rev;
            if (request.getRevision().isPresent()) {
                rev = rs.getRevisionByName(request.getRepositoryId(), request.getRevision().get());
            }
            else {
                rev = rs.getLatestRevision(request.getRepositoryId());
            }

            Path archivePath = createNewArchiveDirectory("revision-" + rev.getName());
            try {
                if (rev.getArchiveType().equals("db")) {  // TODO delegate in-process archive to another class
                    byte[] data = rs.getRevisionArchiveData(rev.getId());
                    try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(data)))) {
                        extractArchive(archivePath, archive);
                    }
                }
                return func.run(archivePath);
            }
            finally {
                deleteFilesIfExistsRecursively(archivePath);
            }
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException("Failed to extract archive", ex);
        }
    }

    private void extractArchive(Path destDir, ArchiveInputStream archive)
        throws IOException
    {
        ArchiveEntry entry;
        while (true) {
            entry = archive.getNextEntry();
            if (entry == null) {
                break;
            }
            Path file = destDir.resolve(entry.getName());
            if (entry.isDirectory()) {
                Files.createDirectories(file);
            }
            else {
                Files.createDirectories(file.getParent());
                try (FileOutputStream out = new FileOutputStream(file.toFile())) {
                    ByteStreams.copy(archive, out);
                }
            }
        }
    }

    private Path createNewArchiveDirectory(String prefix)
        throws IOException
    {
        Files.createDirectories(tempDir);
        return Files.createTempDirectory(tempDir, prefix);
    }

    // TODO copied from io.digdag.server.TempFileManager.deleteFilesIfExistsRecursively
    public static void deleteFilesIfExistsRecursively(Path path)
    {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                {
                    try {
                        Files.deleteIfExists(file);
                    }
                    catch (IOException ex) {
                        // ignore IOException
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                {
                    try {
                        Files.deleteIfExists(dir);
                    }
                    catch (IOException ex) {
                        // ignore IOException
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException ex) {
            // ignore IOException
        }
    }
}

package io.digdag.core.agent;

import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.io.IOException;
import java.io.File;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import com.google.inject.Inject;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.TempFileManager;
import io.digdag.core.TempFileManager.TempDir;
import static io.digdag.core.TempFileManager.deleteFilesIfExistsRecursively;

public class LocalWorkspaceManager
    implements WorkspaceManager
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ProjectStoreManager rm;
    private final TempFileManager tempFiles;

    @Inject
    public LocalWorkspaceManager(ProjectStoreManager rm, TempFileManager tempFiles)
    {
        this.rm = rm;
        this.tempFiles = tempFiles;
    }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, WithWorkspaceAction<T> func)
            throws IOException
    {
        try {
            ProjectStore rs = rm.getProjectStore(request.getSiteId());
            StoredRevision rev;
            if (request.getRevision().isPresent()) {
                rev = rs.getRevisionByName(request.getProjectId(), request.getRevision().get());
            }
            else {
                rev = rs.getLatestRevision(request.getProjectId());
            }

            try (TempDir workspacePath = createNewWorkspace(request)) {
                if (rev.getArchiveType().equals("db")) {  // TODO delegate in-process archive to another class
                    byte[] data = rs.getRevisionArchiveData(rev.getId());
                    try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(data)))) {
                        extractArchive(workspacePath.get(), archive);
                    }
                }
                return func.run(workspacePath.get());
            }
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException("Failed to extract archive", ex);
        }
    }

    private void extractArchive(Path destDir, TarArchiveInputStream archive)
        throws IOException
    {
        String prefix = destDir.toAbsolutePath().normalize().toString();
        TarArchiveEntry entry;
        while (true) {
            entry = archive.getNextTarEntry();
            if (entry == null) {
                break;
            }
            Path path = destDir.resolve(entry.getName()).normalize();
            if (!path.toString().startsWith(prefix)) {
                throw new RuntimeException("Archive includes an invalid entry: " + entry.getName());
            }
            if (entry.isDirectory()) {
                Files.createDirectories(path);
            }
            else if (entry.isSymbolicLink()) {
                Files.createDirectories(path.getParent());
                String dest = entry.getLinkName();
                Path destAbsPath = path.getParent().resolve(dest);
                if (!destAbsPath.normalize().toString().startsWith(prefix)) {
                    throw new RuntimeException("Archive includes an invalid symlink: " + entry.getName() + " -> " + dest);
                }
                Files.createSymbolicLink(path, Paths.get(dest));
            }
            else {
                Files.createDirectories(path.getParent());
                try (OutputStream out = Files.newOutputStream(path)) {
                    ByteStreams.copy(archive, out);
                }
            }
            Files.setPosixFilePermissions(path, getPosixFilePermissions(entry));
        }
    }

    private Set<PosixFilePermission> getPosixFilePermissions(TarArchiveEntry entry)
    {
        int mode = entry.getMode();
        Set<PosixFilePermission> perms = new HashSet<>();
        if ((mode & 0400) != 0) {
            perms.add(PosixFilePermission.OWNER_READ);
        }
        if ((mode & 0200) != 0) {
            perms.add(PosixFilePermission.OWNER_WRITE);
        }
        if ((mode & 0100) != 0) {
            perms.add(PosixFilePermission.OWNER_EXECUTE);
        }
        if ((mode & 0040) != 0) {
            perms.add(PosixFilePermission.GROUP_READ);
        }
        if ((mode & 0020) != 0) {
            perms.add(PosixFilePermission.GROUP_WRITE);
        }
        if ((mode & 0010) != 0) {
            perms.add(PosixFilePermission.GROUP_EXECUTE);
        }
        if ((mode & 0004) != 0) {
            perms.add(PosixFilePermission.OTHERS_READ);
        }
        if ((mode & 0002) != 0) {
            perms.add(PosixFilePermission.OTHERS_WRITE);
        }
        if ((mode & 0001) != 0) {
            perms.add(PosixFilePermission.OTHERS_EXECUTE);
        }
        return perms;
    }

    private TempDir createNewWorkspace(TaskRequest request)
        throws IOException
    {
        return tempFiles.createTempDir("workspace", request.getTaskName());
    }
}

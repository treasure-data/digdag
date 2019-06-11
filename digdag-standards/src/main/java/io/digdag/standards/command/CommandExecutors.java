package io.digdag.standards.command;

import com.google.common.io.ByteStreams;
import io.digdag.client.config.Config;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.spi.TaskRequest;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;

class CommandExecutors
{
    private static final Logger logger = LoggerFactory.getLogger(CommandExecutors.class);

    private CommandExecutors()
    { }

    /**
     * return relative path of project archive
     *
     * @param projectPath
     * @param request
     * @return
     * @throws IOException
     */
    static Path createArchiveFromLocal(
            final ProjectArchiveLoader projectArchiveLoader,
            final Path projectPath,
            final Path ioDirectoryPath,
            final TaskRequest request)
            throws IOException
    {
        final Path archivePath = Files.createTempFile(projectPath.resolve(".digdag/tmp"), "archive-input-", ".tar.gz"); // throw IOException
        logger.debug("Creating " + archivePath + "...");

        final Config config = request.getConfig();
        final WorkflowResourceMatcher workflowResourceMatcher = WorkflowResourceMatcher.defaultMatcher();
        final ProjectArchive projectArchive = projectArchiveLoader.load(projectPath, workflowResourceMatcher, config); // throw IOException
        try (final TarArchiveOutputStream tar = new TarArchiveOutputStream(
                new GzipCompressorOutputStream(Files.newOutputStream(archivePath.toAbsolutePath())))) { // throw IOException
            // Archive project files
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            projectArchive.listFiles((resourceName, absPath) -> { // throw IOException
                logger.debug("  Archiving " + resourceName);
                if (!Files.isDirectory(absPath)) {
                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName); // throw IOException
                    tar.putArchiveEntry(e); // throw IOException
                    if (e.isSymbolicLink()) {
                        logger.debug("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
            });

            // Add .digdag/tmp/ files to the archive
            final Path absoluteIoDirectoryPath = projectPath.resolve(ioDirectoryPath);
            try (final DirectoryStream<Path> ds = Files.newDirectoryStream(absoluteIoDirectoryPath)) {
                for (final Path absPath : ds) {
                    final String resourceName = ProjectArchive.realPathToResourceName(projectPath, absPath);
                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName);
                    tar.putArchiveEntry(e); // throw IOexception
                    if (!e.isSymbolicLink()) {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
            }
        }

        return archivePath;
    }

    static TarArchiveEntry buildTarArchiveEntry(final Path projectPath, final Path absPath, final String name)
            throws IOException
    {
        final TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            final Path rawDest = Files.readSymbolicLink(absPath);
            final Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

            if (!normalizedAbsDest.startsWith(projectPath)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'", normalizedAbsDest, projectPath));
            }

            // absolute path will be invalid on a server. convert it to a relative path
            final Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

            String linkName = normalizedRelativeDest.toString();

            // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
            linkName = linkName.replace(File.separatorChar, '/');
            e.setLinkName(linkName);
        }
        else {
            e = new TarArchiveEntry(absPath.toFile(), name);
            try {
                int mode = 0;
                for (final PosixFilePermission perm : Files.getPosixFilePermissions(absPath)) {
                    switch (perm) {
                        case OWNER_READ:
                            mode |= 0400;
                            break;
                        case OWNER_WRITE:
                            mode |= 0200;
                            break;
                        case OWNER_EXECUTE:
                            mode |= 0100;
                            break;
                        case GROUP_READ:
                            mode |= 0040;
                            break;
                        case GROUP_WRITE:
                            mode |= 0020;
                            break;
                        case GROUP_EXECUTE:
                            mode |= 0010;
                            break;
                        case OTHERS_READ:
                            mode |= 0004;
                            break;
                        case OTHERS_WRITE:
                            mode |= 0002;
                            break;
                        case OTHERS_EXECUTE:
                            mode |= 0001;
                            break;
                        default:
                            // ignore
                    }
                }
                e.setMode(mode);
            }
            catch (UnsupportedOperationException ex) {
                // ignore custom mode
            }
        }
        return e;
    }
}

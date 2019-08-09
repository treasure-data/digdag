package io.digdag.cli.client;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.cli.StdOut;
import io.digdag.cli.YamlMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;

class Archiver
{
    private final PrintStream out;
    private final ConfigFactory cf;
    private final ProjectArchiveLoader projectLoader;

    @Inject
    Archiver(@StdOut PrintStream out, ProjectArchiveLoader projectLoader, ConfigFactory cf)
    {
        this.out = out;
        this.projectLoader = projectLoader;
        this.cf = cf;
    }

    List<String> createArchive(Path projectPath, Path output, boolean copyOutgoingSymlinks)
            throws IOException
    {
        out.println("Creating " + output + "...");

        ProjectArchive project = projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), cf.create());

        ImmutableList.Builder<String> workflowResources = ImmutableList.builder();

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(output)))) {
            // default mode for file names longer than 100 bytes is throwing an exception (LONGFILE_ERROR)
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            project.listFiles((resourceName, absPath) -> {
                TarArchiveEntry e = buildFileOrSymlinkEntryOrNull(projectPath, absPath, resourceName, copyOutgoingSymlinks);
                if (e != null) {
                    tar.putArchiveEntry(e);
                    if (!e.isSymbolicLink()) {
                        try (InputStream in = Files.newInputStream(absPath)) {
                            ByteStreams.copy(in, tar);
                        }
                    }
                    tar.closeArchiveEntry();

                    if (WorkflowResourceMatcher.defaultMatcher().matches(resourceName, absPath)) {
                        workflowResources.add(resourceName);
                    }

                    // If symbolic link entry is created, don't copy files recursively
                    return !e.isSymbolicLink();
                }
                return true;
            });
        }

        return workflowResources.build();
    }

    private TarArchiveEntry buildFileOrSymlinkEntryOrNull(Path projectPath, Path absPath, String resourceName,
            boolean copyOutgoingSymlinks)
            throws IOException
    {
        // If symbolic link, try to create symlink entry
        if (Files.isSymbolicLink(absPath)) {
            TarArchiveEntry e = createSymlinkEntryOrNull(projectPath, absPath, resourceName,
                    copyOutgoingSymlinks);
            if (e != null) {
                out.println("  Archiving " + resourceName);
                out.println("    symlink -> " + e.getLinkName());
                return e;
            }
            // The path is a symbolic link and the file will be copied
        }

        // Skip directories directories because directories will be created automatically
        // when server extracts the archive
        if (Files.isDirectory(absPath)) {
            return null;
        }

        // Create a regular file TarArchiveEntry (follow link if symlink)
        out.println("  Archiving " + resourceName);
        TarArchiveEntry e = new TarArchiveEntry(absPath.toFile(), resourceName);
        try {
            int mode = 0;
            for (PosixFilePermission perm : Files.getPosixFilePermissions(absPath)) {
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

        return e;
    }

    private TarArchiveEntry createSymlinkEntryOrNull(Path projectPath, Path absPath, String resourceName,
            boolean copyOutgoingSymlinks)
            throws IOException
    {
        Path rawDest = Files.readSymbolicLink(absPath);
        Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

        if (!normalizedAbsDest.startsWith(projectPath)) {
            // outside of projectPath
            if (copyOutgoingSymlinks) {
                return null;
            }
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'. Consider to add --copy-outgoing-symlinks option", normalizedAbsDest, projectPath));
        }

        // Create a TarArchiveEntry of symlink
        TarArchiveEntry e = new TarArchiveEntry(resourceName, TarConstants.LF_SYMLINK);

        // absolute path will be invalid on a server. convert it to a relative path
        Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

        String linkName = normalizedRelativeDest.toString();

        // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
        linkName = linkName.replace(File.separatorChar, '/');
        e.setLinkName(linkName);

        return e;
    }
}

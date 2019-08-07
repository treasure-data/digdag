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

    List<String> createArchive(Path projectPath, Path output, boolean copyOutsideSymlinks)
            throws IOException
    {
        out.println("Creating " + output + "...");

        ProjectArchive project = projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), cf.create());

        ImmutableList.Builder<String> workflowResources = ImmutableList.builder();

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(output)))) {
            // default mode for file names longer than 100 bytes is throwing an exception (LONGFILE_ERROR)
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            project.listFiles((resourceName, absPath) -> {
                if (!Files.isDirectory(absPath)) {  // skip directory and symlinks pointing a directory (because NOFOLLOW_LINKS is set)
                    out.println("  Archiving " + resourceName);

                    TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName, copyOutsideSymlinks);
                    tar.putArchiveEntry(e);
                    if (e.isSymbolicLink() && !copyOutsideSymlinks) {
                        out.println("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (InputStream in = Files.newInputStream(absPath)) {
                            ByteStreams.copy(in, tar);
                        }
                    }
                    tar.closeArchiveEntry();

                    if (WorkflowResourceMatcher.defaultMatcher().matches(resourceName, absPath)) {
                        workflowResources.add(resourceName);
                    }
                }
            });
        }

        return workflowResources.build();
    }

    private TarArchiveEntry buildTarArchiveEntry(Path projectPath, Path absPath, String name,
            boolean copyOutsideSymlinks)
            throws IOException
    {
        if (Files.isSymbolicLink(absPath)) {
            TarArchiveEntry symlinkEntry = createSymlinkEntryOrNull(projectPath, absPath, name,
                    copyOutsideSymlinks);
            if (symlinkEntry != null) {
                return symlinkEntry;
            }
        }

        // Create a regular file or directory TarArchiveEntry (follow link if symlink)
        TarArchiveEntry e = new TarArchiveEntry(absPath.toFile(), name);
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

    private TarArchiveEntry createSymlinkEntryOrNull(Path projectPath, Path absPath, String name,
            boolean copyOutsideSymlinks)
            throws IOException
    {
        Path rawDest = Files.readSymbolicLink(absPath);
        Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

        if (!normalizedAbsDest.startsWith(projectPath)) {
            // outside of projectPath
            if (copyOutsideSymlinks) {
                return null;
            }
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'. Consider to add --copy-outside-symlinks option", normalizedAbsDest, projectPath));
        }

        // Create a TarArchiveEntry of symlink
        TarArchiveEntry e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);

        // absolute path will be invalid on a server. convert it to a relative path
        Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

        String linkName = normalizedRelativeDest.toString();

        // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
        linkName = linkName.replace(File.separatorChar, '/');
        e.setLinkName(linkName);

        return e;
    }
}

package io.digdag.cli.client;

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.cli.StdOut;
import io.digdag.cli.YamlMapper;
import io.digdag.client.config.Config;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.repository.WorkflowDefinition;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Date;

class Archiver
{
    private final PrintStream out;
    private final ProjectArchiveLoader projectLoader;
    private final YamlMapper yamlMapper;

    @Inject
    Archiver(@StdOut PrintStream out, ProjectArchiveLoader projectLoader, YamlMapper yamlMapper)
    {
        this.out = out;
        this.projectLoader = projectLoader;
        this.yamlMapper = yamlMapper;
    }

    void createArchive(Path projectPath, Path output, Config overrideParams)
            throws IOException
    {
        out.println("Creating " + output + "...");

        ProjectArchive project = projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), overrideParams);
        ArchiveMetadata meta = project.getArchiveMetadata();

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(output)))) {
            // default mode for file names longer than 100 bytes is throwing an exception (LONGFILE_ERROR)
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            project.listFiles((resourceName, absPath) -> {
                if (!Files.isDirectory(absPath)) {
                    out.println("  Archiving " + resourceName);

                    TarArchiveEntry e = buildTarArchiveEntry(project, absPath, resourceName);
                    tar.putArchiveEntry(e);
                    if (e.isSymbolicLink()) {
                        out.println("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (InputStream in = Files.newInputStream(absPath)) {
                            ByteStreams.copy(in, tar);
                        }
                    }
                    tar.closeArchiveEntry();
                }
            });

            // create .digdag.dig
            // TODO set default time zone if not set?
            byte[] metaBody = yamlMapper.toYaml(meta).getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry metaEntry = new TarArchiveEntry(ArchiveMetadata.FILE_NAME);
            metaEntry.setSize(metaBody.length);
            metaEntry.setModTime(new Date());
            tar.putArchiveEntry(metaEntry);
            tar.write(metaBody);
            tar.closeArchiveEntry();
        }

        out.println("Workflows:");
        for (WorkflowDefinition workflow : meta.getWorkflowList().get()) {
            out.println("  " + workflow.getName());
        }
        out.println("");
    }

    private TarArchiveEntry buildTarArchiveEntry(ProjectArchive project, Path absPath, String name)
            throws IOException
    {
        TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            Path rawDest = Files.readSymbolicLink(absPath);
            Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();
            try {
                project.pathToResourceName(normalizedAbsDest);
            }
            catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid symbolic link: " + ex.getMessage());
            }
            // absolute path will be invalid on a server. convert it to a relative path
            Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

            String linkName = normalizedRelativeDest.toString();

            // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
            linkName = linkName.replace(File.separatorChar, '/');
            e.setLinkName(linkName);
        }
        else {
            e = new TarArchiveEntry(absPath.toFile(), name);
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
        }
        return e;
    }
}

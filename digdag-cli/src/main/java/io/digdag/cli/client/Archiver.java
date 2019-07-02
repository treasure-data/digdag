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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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

    List<String> createArchive(Path projectPath, Path output)
            throws IOException
    {
        out.println("Creating " + output + "...");

        ProjectArchive project = projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), cf.create());

        ImmutableList.Builder<String> workflowResources = ImmutableList.builder();

        List<Path> ignoreFileList = ignoreFileList(projectPath);

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(output)))) {
            // default mode for file names longer than 100 bytes is throwing an exception (LONGFILE_ERROR)
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            project.listFiles((resourceName, absPath) -> {
                if (ignoreFileList.contains(projectPath.resolve(resourceName))) {
                    out.println("  Skip " + resourceName);
                    return;
                }
                if (!Files.isDirectory(absPath)) {
                    out.println("  Archiving " + resourceName);

                    TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName);
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

                    if (WorkflowResourceMatcher.defaultMatcher().matches(resourceName, absPath)) {
                        workflowResources.add(resourceName);
                    }
                }
            });
        }

        return workflowResources.build();
    }

    private TarArchiveEntry buildTarArchiveEntry(Path projectPath, Path absPath, String name)
            throws IOException
    {
        TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            Path rawDest = Files.readSymbolicLink(absPath);
            Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

            if (!normalizedAbsDest.startsWith(projectPath)) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'", normalizedAbsDest, projectPath));
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

    private List<Path> ignoreFileList(Path projectPath) {
        Path ignoreFilePath = projectPath.resolve(".digdagignore");
        if(!ignoreFilePath.toFile().exists()) {
            return new ArrayList<Path>();
        }

        List<Path> ignoreList = new ArrayList<Path>();
        List<Path> noIgnoreList = new ArrayList<Path>();

        try (BufferedReader reader = new BufferedReader(new FileReader(ignoreFilePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try (Stream<Path> entries = Files.walk(projectPath)) {
                    if(line.startsWith("#") || line.startsWith(" ")) {
                        continue;
                    } else if (line.startsWith("!")) {
                        line = line.substring(1, line.length());
                        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + projectPath + "/" + line);
                        entries.filter(matcher::matches).forEach((file) -> noIgnoreList.add(file));
                    } else {
                        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + projectPath + "/" + line);
                        entries.filter(matcher::matches).forEach((file) -> ignoreList.add(file));
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ignoreList.removeAll(noIgnoreList);
        return ignoreList;
    }
}

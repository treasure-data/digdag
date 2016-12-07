package io.digdag.core.archive;

import com.google.common.io.ByteStreams;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class ProjectArchives
{
    private ProjectArchives()
    { }

    public static interface ExtractListener
    {
        void file(Path file);

        void symlink(Path file, String dest);
    }

    public static void extractTarArchive(Path destDir, InputStream in)
        throws IOException
    {
        extractTarArchive(destDir, in, null);
    }

    public static void extractTarArchive(Path destDir, InputStream in, ExtractListener listener)
        throws IOException
    {
        try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(in, 16*1024)))) {
            extractArchive(destDir.toAbsolutePath().normalize(), archive, listener);
        }
    }

    private static void extractArchive(Path destDir, TarArchiveInputStream archive, ExtractListener listener)
        throws IOException
    {
        String prefix = destDir.toString();
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
                Path destAbsPath = path.getParent().resolve(dest).normalize();
                if (!destAbsPath.normalize().toString().startsWith(prefix)) {
                    throw new RuntimeException("Archive includes an invalid symlink: " + entry.getName() + " -> " + dest);
                }
                if (listener != null) {
                    listener.symlink(destDir.relativize(path), dest);
                }
                Files.createSymbolicLink(path, Paths.get(dest));
            }
            else {
                Files.createDirectories(path.getParent());
                if (listener != null) {
                    listener.file(destDir.relativize(path));
                }
                try (OutputStream out = Files.newOutputStream(path)) {
                    ByteStreams.copy(archive, out);
                }
            }
            Files.setPosixFilePermissions(path, getPosixFilePermissions(entry));
        }
    }

    private static Set<PosixFilePermission> getPosixFilePermissions(TarArchiveEntry entry)
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
}

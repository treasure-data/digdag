package io.digdag.server.rs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitResult;
import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;

public class TempFileManager
{
    private final Path path;

    @Inject  // TODO configurable
    public TempFileManager()
    {
        try {
            this.path = Files.createTempDirectory("temp");
        }
        catch (IOException ex) {
            throw new TempFileException(ex);
        }
    }

    public TempFileManager(File path)
    {
        this.path = path.toPath();
    }

    public TempFile createTempFile()
    {
        return createTempFile("tmp");
    }

    public TempFile createTempFile(String suffix)
    {
        return createTempFile(Thread.currentThread().getName()+"_", suffix);
    }

    public TempFile createTempFile(String prefix, String suffix)
    {
        try {
            Files.createDirectories(path);
            return new TempFile(Files.createTempFile(path, prefix, suffix).toFile());
        }
        catch (IOException ex) {
            throw new TempFileException(ex);
        }
    }

    public TempDir createTempDir()
    {
        return createTempDir(Thread.currentThread().getName()+"_");
    }

    public TempDir createTempDir(String prefix)
    {
        try {
            Files.createDirectories(path);
            return new TempDir(Files.createTempDirectory(path, prefix).toFile());
        }
        catch (IOException ex) {
            throw new TempFileException(ex);
        }
    }

    public static class TempFile
            implements AutoCloseable
    {
        private final File file;

        TempFile(File file)
        {
            this.file = file;
        }

        public File get()
        {
            return file;
        }

        @Override
        public void close()
        {
            deleteFilesIfExistsRecursively(file);
        }
    }

    public static class TempDir
            implements AutoCloseable
    {
        private final File dir;

        TempDir(File dir)
        {
            this.dir = dir;
        }

        public File get()
        {
            return dir;
        }

        public File child(String child)
        {
            return new File(dir, child);
        }

        @Override
        public void close()
        {
            deleteFilesIfExistsRecursively(dir);
        }
    }

    public static void deleteFilesIfExistsRecursively(File dir)
    {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>()
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

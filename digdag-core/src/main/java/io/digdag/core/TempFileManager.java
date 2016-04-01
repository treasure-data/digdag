package io.digdag.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitResult;
import java.io.File;
import java.io.Closeable;
import java.io.IOException;
import com.google.inject.Inject;

public class TempFileManager
{
    public static class AllocationException
            extends RuntimeException
    {
        public AllocationException(IOException cause)
        {
            super(cause);
        }

        @Override
        public IOException getCause()
        {
            return (IOException) super.getCause();
        }
    }

    private final Path dir;

    @Inject
    public TempFileManager()
    {
        try {
            this.dir = Files.createTempDirectory("temp");
        }
        catch (IOException ex) {
            throw new AllocationException(ex);
        }
    }

    public TempFileManager(Path dir)
    {
        this.dir = dir;
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
            Files.createDirectories(dir);
            return new TempFile(Files.createTempFile(dir, prefix, suffix));
        }
        catch (IOException ex) {
            throw new AllocationException(ex);
        }
    }

    public TempDir createTempDir()
    {
        return createTempDir(Thread.currentThread().getName()+"_");
    }

    public TempDir createTempDir(String prefix)
    {
        try {
            Files.createDirectories(dir);
            return new TempDir(Files.createTempDirectory(dir, prefix + "_"));
        }
        catch (IOException ex) {
            throw new AllocationException(ex);
        }
    }

    public TempDir createTempDir(String subdirName, String prefix)
    {
        try {
            Path subdir = dir.resolve(subdirName);
            Files.createDirectories(subdir);
            return new TempDir(Files.createTempDirectory(subdir, prefix + "_"));
        }
        catch (IOException ex) {
            throw new AllocationException(ex);
        }
    }

    public static class TempFile
            implements Closeable
    {
        private final Path path;

        TempFile(Path path)
        {
            this.path = path;
        }

        public Path get()
        {
            return path;
        }

        @Override
        public void close()
        {
            deleteFilesIfExistsRecursively(path);
        }
    }

    public static class TempDir
            implements Closeable
    {
        private final Path path;

        TempDir(Path path)
        {
            this.path = path;
        }

        public Path get()
        {
            return path;
        }

        public Path child(String child)
        {
            return path.resolve(child);
        }

        @Override
        public void close()
        {
            deleteFilesIfExistsRecursively(path);
        }
    }

    public static void deleteFilesIfExistsRecursively(Path dir)
    {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>()
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

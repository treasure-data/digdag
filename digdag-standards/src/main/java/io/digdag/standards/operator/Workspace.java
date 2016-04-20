package io.digdag.standards.operator;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.Charset;

public class Workspace
    implements Closeable
{
    private final Path path;
    private final List<String> tempFiles = new ArrayList<>();

    public Workspace(Path path)
    {
        this.path = path;
    }

    public Path getPath()
    {
        return path;
    }

    public String createTempFile(String prefix, String suffix)
        throws IOException
    {
        // file will be deleted by WorkspaceManager
        Path file = Files.createTempFile(getTempDir(), prefix, suffix);
        String relative = path.relativize(file).toString();
        tempFiles.add(relative);
        return relative;
    }

    public InputStream newInputStream(String relative)
        throws IOException
    {
        return Files.newInputStream(getPath(relative));
    }

    public BufferedReader newBufferedReader(String relative, Charset cs)
        throws IOException
    {
        return Files.newBufferedReader(getPath(relative), cs);
    }

    public OutputStream newOutputStream(String relative)
        throws IOException
    {
        return Files.newOutputStream(getPath(relative));
    }

    public BufferedWriter newBufferedWriter(String relative, Charset cs)
        throws IOException
    {
        return Files.newBufferedWriter(getPath(relative), cs);
    }

    public Path getPath(String relative)
    {
        return path.resolve(relative);
    }

    public File getFile(String relative)
    {
        return getPath(relative).toFile();
    }

    private synchronized Path getTempDir()
        throws IOException
    {
        Path dir = path.resolve(".digdag/tmp");
        Files.createDirectories(dir);
        return dir;
    }

    @Override
    public void close()
    {
        for (String relative : tempFiles) {
            try {
                Files.deleteIfExists(getPath(relative));
            }
            catch (IOException ex) {
                // TODO show warning log
            }
        }
    }
}

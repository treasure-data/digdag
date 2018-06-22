package io.digdag.spi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CommandExecutorContent // Scalable immutable byte array with
{
    public static CommandExecutorContent create(final Path workspacePath, final String relativePath)
    {
        final Path path = workspacePath.resolve(relativePath);
        final File file = path.toFile();
        final long contentLength = file.length();
        return new CommandExecutorContent(relativePath, contentLength);
    }

    private final String name;
    private final long contentLength;

    private CommandExecutorContent(String name, long contentLength)
    {
        this.name = name;
        this.contentLength = contentLength;
    }

    public long getContentLength()
    {
        return contentLength;
    }

    public String getName()
    {
        return name;
    }
}

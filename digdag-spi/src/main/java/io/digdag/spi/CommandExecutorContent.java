package io.digdag.spi;

import java.io.File;
import java.nio.file.Path;

public class CommandExecutorContent // Scalable immutable byte array with
{
    public static CommandExecutorContent create(final Path workspacePath, final String relativePath)
    {
        final File file = workspacePath.resolve(relativePath).toFile();
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

    // TODO more SPI
}

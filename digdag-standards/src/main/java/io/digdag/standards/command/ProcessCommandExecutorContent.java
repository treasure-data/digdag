package io.digdag.standards.command;

import io.digdag.spi.CommandExecutorContent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ProcessCommandExecutorContent
    implements CommandExecutorContent
{
    public static CommandExecutorContent create(final Path workspacePath, final String relativePath)
            throws IOException
    {
        final Path path = workspacePath.resolve(relativePath);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final long length = Files.copy(path, out);
        return new ProcessCommandExecutorContent(length, out.toByteArray());
    }

    private final long length;
    private final byte[] bytes;

    private ProcessCommandExecutorContent(final long length, final byte[] bytes)
    {
        this.length = length;
        this.bytes = bytes;
    }

    public long getLength()
    {
        return length;
    }

    public InputStream newInputStream()
            throws IOException
    {

        return new ByteArrayInputStream(bytes);
    }
}

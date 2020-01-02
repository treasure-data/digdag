package io.digdag.core.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.zip.GZIPOutputStream;

public class CountingLogOutputStream
    extends GZIPOutputStream
{
    private final Path path;
    private final Instant openTime;
    private int count;

    public CountingLogOutputStream(Path path)
        throws IOException
    {
        super(Files.newOutputStream(path), 8*1024);
        this.def.setLevel(9);
        this.path = path;
        this.openTime = Instant.now();
    }

    @Override
    public void write(int b) throws IOException
    {
        super.write(b);
        count++;
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException
    {
        super.write(b, off, len);
        count += len;
    }

    public Path getPath()
    {
        return path;
    }

    public Instant getOpenTime()
    {
        return openTime;
    }

    public int getUncompressedSize()
    {
        return count;
    }
}

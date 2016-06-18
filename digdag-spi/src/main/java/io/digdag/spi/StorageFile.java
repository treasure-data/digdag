package io.digdag.spi;

import java.io.InputStream;

public class StorageFile
{
    private final InputStream inputStream;
    private final long contentLength;

    public StorageFile(InputStream inputStream, long contentLength)
    {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
    }

    public InputStream getContentInputStream()
    {
        return inputStream;
    }

    public long getContentLength()
    {
        return contentLength;
    }
}


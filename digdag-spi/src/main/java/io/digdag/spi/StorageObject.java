package io.digdag.spi;

import java.io.InputStream;

public class StorageObject
{
    private final InputStream inputStream;
    private final long contentLength;

    public StorageObject(InputStream inputStream, long contentLength)
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


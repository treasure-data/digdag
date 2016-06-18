package io.digdag.spi;

import java.util.Objects;
import java.util.Arrays;
import java.time.Instant;

public class StorageFileMetadata
{
    private final String key;
    private final long contentLength;
    private final Instant lastModified;

    public StorageFileMetadata(String key, long contentLength, Instant lastModified)
    {
        this.key = key;
        this.contentLength = contentLength;
        this.lastModified = lastModified;
    }

    public String getKey() {
        return key;
    }

    public long getContentLength()
    {
        return contentLength;
    }

    public Instant getLastModified()
    {
        return lastModified;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(new Object[] {
            key,
            contentLength,
            lastModified,
        });
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof StorageFileMetadata)) {
            return false;
        }
        StorageFileMetadata o = (StorageFileMetadata) obj;
        return Objects.equals(key, o.key) &&
            Objects.equals(contentLength, o.contentLength) &&
            Objects.equals(lastModified, o.lastModified);
    }
}

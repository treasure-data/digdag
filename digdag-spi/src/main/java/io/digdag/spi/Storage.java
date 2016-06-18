package io.digdag.spi;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;

public interface Storage
{
    StorageFile open(String key)
        throws StorageFileNotFoundException;

    public interface UploadStreamProvider
    {
        public InputStream open() throws IOException;
    }

    String put(String key, long contentLength,
            UploadStreamProvider payload)
        throws IOException;

    interface FileListing
    {
        void accept(List<StorageFileMetadata> chunk);
    }

    void list(String bucket, String keyPrefix, FileListing callback);
}

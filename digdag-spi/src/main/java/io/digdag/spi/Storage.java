package io.digdag.spi;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import com.google.common.base.Optional;

public interface Storage
{
    StorageObject open(String key)
        throws StorageFileNotFoundException;

    interface UploadStreamProvider
    {
        InputStream open() throws IOException;
    }

    String put(String key, long contentLength,
            UploadStreamProvider payload)
        throws IOException;

    interface FileListing
    {
        void accept(List<StorageObjectSummary> chunk);
    }

    void list(String keyPrefix, FileListing callback);

    default Optional<DirectDownloadHandle> getDirectDownloadHandle(String key)
    {
        return Optional.absent();
    }

    default Optional<DirectUploadHandle> getDirectUploadHandle(String key)
    {
        return Optional.absent();
    }

    Long getDirectDownloadExpiration();

    Long getDirectUploadExpiration();
}

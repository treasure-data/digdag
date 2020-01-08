package io.digdag.storage.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.spi.StorageObject;
import io.digdag.spi.StorageObjectSummary;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.net.URL;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class GCSStorage
        implements io.digdag.spi.Storage
{
    private static Logger logger = LoggerFactory.getLogger(GCSStorage.class);

    private final Config config;
    private final com.google.cloud.storage.Storage storage;
    private final String bucket;

    public GCSStorage(final Config config, com.google.cloud.storage.Storage storage, String bucket)
    {
        checkArgument(!isNullOrEmpty(bucket), "bucket is null or empty");
        this.config = config;
        this.storage = storage;
        this.bucket = bucket;
    }

    private RetryExecutor uploadRetryExecutor()
    {
        return RetryExecutor.retryExecutor();
    }

    private RetryExecutor getRetryExecutor()
    {
        return RetryExecutor.retryExecutor();
    }

    @Override
    public StorageObject open(String object)
    {
        checkArgument(object != null, "object is null");
        Blob blob = storage.get(bucket, object);
        String errorMessage = "opening file bucket " + bucket + " key " + object;
        byte[] content = getWithRetry(errorMessage, () -> blob.getContent());
        InputStream byteStream = new ByteArrayInputStream(content);
        return new StorageObject(byteStream, content.length);
    }

    @Override
    public String put(String object, long contentLength,
            UploadStreamProvider payload)
            throws IOException
    {
        checkArgument(object != null, "object is null");
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, object).build();
        try {
            return uploadRetryExecutor()
                    .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                        logger.warn("Retrying uploading file bucket " + bucket + " object " + object + " error: " + exception);
                    })
                    .retryIf((exception) -> {
                        if (exception instanceof IOException || exception instanceof InterruptedException) {
                            return false;
                        }
                        return true;
                    })
                    .runInterruptible(() -> {
                        try (InputStream in = payload.open()) {
                            try (WriteChannel writer = storage.writer(blobInfo)) {
                                byte[] buffer = new byte[1024];
                                int limit;
                                while ((limit = in.read(buffer)) >= 0) {
                                    try {
                                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                                    }
                                    catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                }
                            }
                            return storage.get(bucket, object).getMd5ToHexString();
                        }
                    });
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        catch (RetryGiveupException ex) {
            Throwable cause = ex.getCause();
            Throwables.propagateIfInstanceOf(cause, IOException.class);
            throw Throwables.propagate(cause);
        }
    }

    @Override
    public void list(String objectPrefix, FileListing callback)
    {
        checkArgument(objectPrefix != null, "objectPrefix is null");

        String errorMessage = "listing files on bucket " + bucket + " prefix " + objectPrefix;
        Page<Blob> blobs = getWithRetry(errorMessage, () ->
                storage.list(bucket, BlobListOption.prefix(objectPrefix))
        );

        List<StorageObjectSummary> objectSummaryList = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            objectSummaryList.add(
                    StorageObjectSummary.builder()
                            .key(blob.getName())
                            .contentLength(blob.getSize())
                            .lastModified(convertToInstant(blob))
                            .build()
            );
        }
        callback.accept(objectSummaryList);
    }

    @Override
    public Optional<DirectDownloadHandle> getDirectDownloadHandle(String object)
    {
        final long secondsToExpire = config.get("direct_download_expiration", Long.class, 10L*60);

        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, object).build();
        URL signedUrl = this.storage.signUrl(blobInfo, secondsToExpire, TimeUnit.SECONDS, Storage.SignUrlOption.withV4Signature());
        String url = signedUrl.toString();

        return Optional.of(DirectDownloadHandle.of(url));
    }

    @Override
    public Optional<DirectUploadHandle> getDirectUploadHandle(String object)
    {
        final long secondsToExpire = config.get("direct_upload_expiration", Long.class, 10L*60);

        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, object).build();
        URL signedUrl = this.storage.signUrl(blobInfo, secondsToExpire, TimeUnit.SECONDS, Storage.SignUrlOption.withV4Signature());
        String url = signedUrl.toString();

        return Optional.of(DirectUploadHandle.of(url));
    }

    private <T> T getWithRetry(String message, Callable<T> callable)
            throws StorageException
    {
        try {
            return getRetryExecutor()
                    .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                        logger.warn(String.format("Retrying %s (%d/%d): %s", message, retryCount, retryLimit, exception));
                    })
                    .retryIf((exception) -> !isNotFoundException(exception))
                    .runInterruptible(() -> callable.call());
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        catch (RetryGiveupException ex) {
            Exception cause = ex.getCause();
            throw Throwables.propagate(cause);
        }
    }

    private static Instant convertToInstant(Blob blob){
        try {
            return Instant.ofEpochMilli(blob.getUpdateTime());
        } catch (NullPointerException e) {
            // NOTE: 1970-01-01T00:00:00Z
            return Instant.ofEpochMilli(0L);
        }
    }

    private static boolean isNotFoundException(Exception ex)
    {
        // This includes NoSuchBucket and NoSuchKey. See also:
        // https://cloud.google.com/storage/docs/json_api/v1/status-codes
        return ex instanceof StorageException &&
                ((StorageException) ex).getCode() == 404;
    }
}

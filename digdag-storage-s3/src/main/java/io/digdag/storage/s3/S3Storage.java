package io.digdag.storage.s3;

import java.util.Date;
import java.time.Instant;
import java.io.IOException;
import java.io.InputStream;
import java.io.FilterInputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Callable;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.amazonaws.HttpMethod;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import io.digdag.client.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObject;
import io.digdag.spi.StorageObjectSummary;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import io.digdag.util.ResumableInputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class S3Storage
    implements Storage
{
    private static Logger logger = LoggerFactory.getLogger(S3Storage.class);

    private final Config config;
    private final AmazonS3Client client;
    private final String bucket;
    private final ExecutorService uploadExecutor;
    private final TransferManager transferManager;

    public S3Storage(final Config config, AmazonS3Client client, String bucket)
    {
        checkArgument(!isNullOrEmpty(bucket), "bucket is null or empty");
        this.config = config;
        this.client = client;
        this.bucket = bucket;
        this.uploadExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                    .setNameFormat("storage-s3-upload-transfer-%d")
                    .build());
        this.transferManager = new TransferManager(client, uploadExecutor);
        // TODO check the existence of the bucket so that following
        //      any GET or PUT don't get 404 Not Found error.
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
    public StorageObject open(String key)
        throws StorageFileNotFoundException
    {
        checkArgument(key != null, "key is null");

        String errorMessage = "opening file bucket " + bucket + " key " + key;
        GetObjectRequest req = new GetObjectRequest(bucket, key);

        S3Object obj = getWithRetry(errorMessage, () -> client.getObject(req));

        final long actualSize = obj.getObjectMetadata().getContentLength();

        // override close to call abort instead because close skips all remaining bytes so that
        // s3 client can reuse the TCP connection. but close of a fully opened file is occasionally
        // used to skip remaining work (e.g. finally block when exception is thrown). Unlike openRange,
        // performance impact could be significantly large.
        InputStream stream = overrideCloseToAbort(obj.getObjectContent());

        InputStream resumable = new ResumableInputStream(stream, (offset, closedCause) -> {
                try {
                    S3ObjectInputStream raw = getWithRetry(errorMessage, () -> {
                            req.setRange(offset, actualSize - offset - 1);
                            return client.getObject(req);
                        })
                    .getObjectContent();
                    return overrideCloseToAbort(raw);
                }
                catch (StorageFileNotFoundException ex) {
                    throw new IOException(ex);
                }
            });

        return new StorageObject(resumable, actualSize);
    }

    private InputStream overrideCloseToAbort(final S3ObjectInputStream raw)
    {
        return new FilterInputStream(raw)
        {
            @Override
            public void close() throws IOException
            {
                raw.abort();
            }
        };
    }

    @Override
    public String put(String key, long contentLength,
            UploadStreamProvider payload)
        throws IOException
    {
        checkArgument(key != null, "key is null");

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(contentLength);

        try {
            return uploadRetryExecutor()
                .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                    logger.warn("Retrying uploading file bucket "+bucket+" key "+key+" error: "+exception);
                })
                .retryIf((exception) -> {
                    if (exception instanceof IOException || exception instanceof InterruptedException) {
                        return false;
                    }
                    return true;
                })
                .runInterruptible(() -> {
                    try (InputStream in = payload.open()) {
                        PutObjectRequest req = new PutObjectRequest(bucket, key, in, meta);
                        UploadResult result = transferManager.upload(req).waitForUploadResult();

                        return result.getETag();
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
    public void list(String keyPrefix, FileListing callback)
    {
        checkArgument(keyPrefix != null, "keyPrefix is null");

        String errorMessage = "listing files on bucket " + bucket + " prefix " + keyPrefix;

        ListObjectsRequest req = new ListObjectsRequest();
        req.setBucketName(bucket);
        req.setPrefix(keyPrefix);

        ObjectListing listing;
        do {
            try {
                listing = getWithRetry(errorMessage, () -> client.listObjects(req));
            }
            catch (StorageFileNotFoundException ex) {
                throw Throwables.propagate(ex.getCause());
            }
            callback.accept(Lists.transform(
                        listing.getObjectSummaries(),
                        (summary) -> StorageObjectSummary.builder()
                            .key(summary.getKey())
                            .contentLength(summary.getSize())
                            .lastModified(summary.getLastModified().toInstant())
                            .build()
                        ));
            req.setMarker(listing.getNextMarker());
        }
        while (listing.isTruncated());
    }

    @Override
    public Optional<DirectDownloadHandle> getDirectDownloadHandle(String key)
    {
        final long secondsToExpire = getDirectUploadExpiration();

        GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucket, key);
        req.setExpiration(Date.from(Instant.now().plusSeconds(secondsToExpire)));

        String url = client.generatePresignedUrl(req).toString();

        return Optional.of(DirectDownloadHandle.of(url));
    }

    @Override
    public Optional<DirectUploadHandle> getDirectUploadHandle(String key)
    {
        final long secondsToExpire = getDirectUploadExpiration();

        GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucket, key);
        req.setMethod(HttpMethod.PUT);
        req.setExpiration(Date.from(Instant.now().plusSeconds(secondsToExpire)));

        String url = client.generatePresignedUrl(req).toString();

        return Optional.of(DirectUploadHandle.of(url));
    }

    @Override
    public Long getDirectDownloadExpiration()
    {
        return config.get("direct_download_expiration", Long.class, 10L*60);
    }

    @Override
    public Long getDirectUploadExpiration()
    {
        return config.get("direct_upload_expiration", Long.class, 10L*60);
    }

    private <T> T getWithRetry(String message, Callable<T> callable)
        throws StorageFileNotFoundException
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
            if (isNotFoundException(cause)) {
                throw new StorageFileNotFoundException("S3 file not found", cause);
            }
            throw Throwables.propagate(cause);
        }
    }

    private static boolean isNotFoundException(Exception ex)
    {
        // This includes NoSuchBucket and NoSuchKey. See also:
        // http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        return ex instanceof AmazonServiceException &&
            ((AmazonServiceException) ex).getStatusCode() == 404;
    }
}

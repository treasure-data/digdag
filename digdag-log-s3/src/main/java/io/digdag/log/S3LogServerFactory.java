package io.digdag.log;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Date;
import java.time.Instant;
import com.google.inject.Inject;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogFileHandle;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.client.config.Config;
import io.digdag.core.log.AbstractFileLogServer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ListObjectsRequest;

public class S3LogServerFactory
    implements LogServerFactory
{
    private final AmazonS3Client s3;
    private final String bucket;
    private final String logPath;

    @Inject
    public S3LogServerFactory(Config systemConfig)
    {
        this.s3 = new AmazonS3Client(
                buildCredentialsProvider(systemConfig),
                buildClientConfiguration(systemConfig));
        if (systemConfig.has("log-server.s3.endpoint")) {
            s3.setEndpoint(systemConfig.get("log-server.s3.endpoint", String.class));
        }

        this.bucket = systemConfig.get("log-server.s3.bucket", String.class);
        String logPath = systemConfig.get("log-server.s3.path", String.class, "");
        if (logPath.startsWith("/")) {
            logPath = logPath.substring(1);
        }
        if (!logPath.endsWith("/") && !logPath.isEmpty()) {
            logPath = logPath + "/";
        }
        this.logPath = logPath;
    }

    private static ClientConfiguration buildClientConfiguration(Config systemConfig)
    {
        // TODO build from systemConfig with log-server.s3.client prefix
        return new ClientConfiguration();
    }

    private static AWSCredentialsProvider buildCredentialsProvider(Config systemConfig)
    {
        if (systemConfig.has("log-server.s3.credentials.file")) {
            return new PropertiesFileCredentialsProvider(
                    systemConfig.get("log-server.s3.credentials.file", String.class));
        }
        else if (systemConfig.has("log-server.s3.credentials.access-key-id")) {
            final BasicAWSCredentials creds = new BasicAWSCredentials(
                systemConfig.get("log-server.s3.credentials.access-key-id", String.class),
                systemConfig.get("log-server.s3.credentials.secret-access-key", String.class));
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials()
                {
                    return creds;
                }

                @Override
                public void refresh()
                { }
            };
        }
        else {
            return new DefaultAWSCredentialsProviderChain();
        }
    }

    @Override
    public String getType()
    {
        return "s3";
    }

    @Override
    public LogServer getLogServer()
    {
        return new S3FileLogServer();
    }

    public class S3FileLogServer
            extends AbstractFileLogServer
    {
        private String getPrefixDir(String dateDir, String attemptDir)
        {
            return logPath + dateDir + "/" + attemptDir + "/";
        }

        @Override
        public Optional<DirectUploadHandle> getDirectUploadHandle(String dateDir, String attemptDir, String fileName)
        {
            String path = getPrefixDir(dateDir, attemptDir) + fileName;

            GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucket, path);
            req.setMethod(HttpMethod.PUT);
            req.setExpiration(
                    Date.from(Instant.now().plusSeconds(10*60)));

            String url = s3.generatePresignedUrl(req).toString();

            return Optional.of(
                    DirectUploadHandle.builder()
                    .type("http")
                    .url(url)
                    .build());
        }

        @Override
        protected void putFile(String dateDir, String attemptDir, String fileName, byte[] gzData)
        {
            String path = getPrefixDir(dateDir, attemptDir) + fileName;

            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(gzData.length);

            // TODO implement retrying by using RetryExecutor in digdag-plugin-utils
            try {
                try (InputStream in = new ByteArrayInputStream(gzData)) {
                    Object r = s3.putObject(new PutObjectRequest(bucket, path, in, meta));
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
                catch (Exception ex) {
                    throw Throwables.propagate(ex);
                }
            }
            catch (Throwable ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        protected byte[] getFile(String dateDir, String attemptDir, String fileName)
            throws FileNotFoundException
        {
            String path = getPrefixDir(dateDir, attemptDir) + fileName;

            // TODO implement retrying by using ResumableInputStream from embulk
            GetObjectRequest req = new GetObjectRequest(bucket, path);
            try {
                S3Object obj = s3.getObject(req);
                try (InputStream in = obj.getObjectContent()) {
                    long size = obj.getObjectMetadata().getContentLength();
                    if (size > 512*1024*1024) {
                        throw new RuntimeException("Non-direct downloding log files larger than 512MB is not supported");
                    }
                    byte[] data = new byte[(int) size];
                    ByteStreams.readFully(in, data);
                    return data;
                }
            }
            catch (AmazonServiceException ex) {
                // Status code 404 includes NoSuchBucket and NoSuchKey. See also:
                // http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.getStatusCode() == 404) {
                    throw new FileNotFoundException();
                }
                throw ex;
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        protected void listFiles(String dateDir, String attemptDir, FileMetadataConsumer consumer)
        {
            String dir = getPrefixDir(dateDir, attemptDir);

            GeneratePresignedUrlRequest genReq = new GeneratePresignedUrlRequest(bucket, "");
            genReq.setBucketName(bucket);
            genReq.setExpiration(Date.from(Instant.now().plusSeconds(10*60)));

            ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucket).withPrefix(dir);
            ObjectListing listing;
            do {
                listing = s3.listObjects(req);
                listing.getObjectSummaries().stream()
                    .forEach(summary -> {
                        String key = summary.getKey();
                        String fileName = key.substring(dir.length());

                        genReq.setKey(key);
                        String url = s3.generatePresignedUrl(genReq).toString();

                        DirectDownloadHandle directDownload = DirectDownloadHandle.builder()
                                .type("http")
                                .url(url)
                                .build();

                        consumer.accept(
                                fileName,
                                summary.getSize(),
                                directDownload);
                    });
                req.setMarker(listing.getNextMarker());
            } while (listing.isTruncated());
        }
    }
}

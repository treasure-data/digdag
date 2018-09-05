package io.digdag.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFactory;

public class S3StorageFactory
    implements StorageFactory
{
    @Override
    public String getType()
    {
        return "s3";
    }

    @Override
    public Storage newStorage(Config config)
    {
        AmazonS3Client client = new AmazonS3Client(
                buildCredentialsProvider(config),
                buildClientConfiguration(config));
        if (config.has("endpoint")) {
            client.setEndpoint(config.get("endpoint", String.class));
        }

        if (config.has("path-style-access")) {
            client.setS3ClientOptions(
              S3ClientOptions.builder().setPathStyleAccess(
                config.get("path-style-access", Boolean.class, false)
              ).build());
        }

        String bucket = config.get("bucket", String.class);

        return new S3Storage(config, client, bucket);
    }

    private static ClientConfiguration buildClientConfiguration(Config config)
    {
        // TODO build from config with log-server.s3.client prefix
        return new ClientConfiguration();
    }

    private static AWSCredentialsProvider buildCredentialsProvider(Config config)
    {
        if (config.has("credentials.file")) {
            return new PropertiesFileCredentialsProvider(
                    config.get("credentials.file", String.class));
        }
        else if (config.has("credentials.access-key-id")) {
            final BasicAWSCredentials creds = new BasicAWSCredentials(
                config.get("credentials.access-key-id", String.class),
                config.get("credentials.secret-access-key", String.class));
            return new AWSCredentialsProvider()
            {
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
}

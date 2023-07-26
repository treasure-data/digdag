package io.digdag.standards.command.ecs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import io.digdag.client.config.ConfigException;

public class DefaultEcsClientFactory
        implements EcsClientFactory
{
    @Override
    public EcsClient createClient(final EcsClientConfig ecsClientConfig)
            throws ConfigException
    {
        final AWSCredentialsProvider credentials;
        if (ecsClientConfig.getAccessKeyId().isPresent()) {
                credentials = new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                                ecsClientConfig.getAccessKeyId().get(),
                                ecsClientConfig.getSecretAccessKey().get()
                                )
                        );
                }
        else {
                credentials = new DefaultAWSCredentialsProviderChain();
        }

        // TODO improve to enable more options
        final ClientConfiguration clientConfig = new ClientConfiguration()
                .withProtocol(Protocol.HTTPS)
                .withMaxErrorRetry(ecsClientConfig.getMaxRetries());

        final AmazonECSClient ecsClient = (AmazonECSClient) AmazonECSClient.builder()
                .withRegion(ecsClientConfig.getRegion())
                .withCredentials(credentials)
                .withClientConfiguration(clientConfig)
                .build();

        final AWSLogs logsClient = AWSLogsClient.builder()
                .withRegion(ecsClientConfig.getRegion())
                .withCredentials(credentials)
                .withClientConfiguration(clientConfig)
                .build();

        return new DefaultEcsClient(ecsClientConfig, ecsClient, logsClient);
    }
}

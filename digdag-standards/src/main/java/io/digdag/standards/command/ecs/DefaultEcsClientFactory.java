package io.digdag.standards.command.ecs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import io.digdag.client.config.ConfigException;

import javax.validation.constraints.NotNull;

public class DefaultEcsClientFactory
        implements EcsClientFactory
{
    @Override
    public EcsClient createClient(final EcsClientConfig ecsClientConfig)
            throws ConfigException
    {
        // TODO improve to enable several types of credentials providers
        final AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ecsClientConfig.getAccessKeyId(), ecsClientConfig.getSecretAccessKey()));
        // TODO improve to enable more options
        final ClientConfiguration clientConfig = new ClientConfiguration()
                .withProtocol(Protocol.HTTPS);

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

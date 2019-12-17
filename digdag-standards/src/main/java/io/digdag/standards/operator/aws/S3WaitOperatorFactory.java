package io.digdag.standards.operator.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.PollingTimeoutException;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

public class S3WaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(S3WaitOperatorFactory.class);

    private static final DurationInterval POLL_INTERVAL = DurationInterval.of(Duration.ofSeconds(5), Duration.ofMinutes(5));
    private static final double NEXT_INTERVAL_EXP_BASE = 1.2; // interval time will increase 5sec *1.2**N until 5min.

    private final AmazonS3ClientFactory s3ClientFactory;
    private final Map<String, String> environment;

    @Inject
    public S3WaitOperatorFactory(@Environment Map<String, String> environment)
    {
        this(AmazonS3Client::new, environment);
    }

    @VisibleForTesting
    S3WaitOperatorFactory(
            AmazonS3ClientFactory s3ClientFactory,
            Map<String, String> environment)
    {
        this.s3ClientFactory = s3ClientFactory;
        this.environment = environment;
    }

    public String getType()
    {
        return "s3_wait";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new S3WaitOperator(context);
    }

    private class S3WaitOperator
            implements Operator
    {
        private final TaskRequest request;
        private final TaskState state;
        private final SecretProvider secrets;

        public S3WaitOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
            this.state = TaskState.of(request);
            this.secrets = context.getSecrets();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws").getNestedOrGetEmpty("s3"))
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws"));

            Optional<String> command = params.getOptional("_command", String.class);

            Optional<String> bucket = params.getOptional("bucket", String.class);
            Optional<String> key = params.getOptional("key", String.class);
            Optional<String> versionId = params.getOptional("version_id", String.class);
            Optional<Boolean> pathStyleAccess = params.getOptional("path_style_access", Boolean.class);
            Optional<Duration> timeout = params.getOptional("timeout", String.class)
                                            .transform( (t) -> Durations.parseDuration(t));
            boolean ignoreTimeoutError = params.getOptional("ignore_timeout_error", Boolean.class).or(false);

            if (command.isPresent() && (bucket.isPresent() || key.isPresent()) ||
                    !command.isPresent() && (!bucket.isPresent() || !key.isPresent())) {
                throw new ConfigException("Either the s3_wait operator command or both 'bucket' and 'key' parameters must be set");
            }

            if (command.isPresent()) {
                List<String> parts = Splitter.on('/').limit(2).splitToList(command.get());
                if (parts.size() != 2) {
                    throw new ConfigException("Illegal s3 path: " + command.get());
                }
                bucket = Optional.of(parts.get(0));
                key = Optional.of(parts.get(1));
            }

            SecretProvider awsSecrets = secrets.getSecrets("aws");
            SecretProvider s3Secrets = awsSecrets.getSecrets("s3");

            Optional<String> endpoint = Aws.first(
                    () -> s3Secrets.getSecretOptional("endpoint"),
                    () -> params.getOptional("endpoint", String.class));

            Optional<String> regionName = Aws.first(
                    () -> s3Secrets.getSecretOptional("region"),
                    () -> awsSecrets.getSecretOptional("region"),
                    () -> params.getOptional("region", String.class));

            String accessKey = s3Secrets.getSecretOptional("access_key_id")
                    .or(() -> awsSecrets.getSecret("access_key_id"));

            String secretKey = s3Secrets.getSecretOptional("secret_access_key")
                    .or(() -> awsSecrets.getSecret("secret_access_key"));

            // Create S3 Client
            ClientConfiguration configuration = new ClientConfiguration();
            Aws.configureProxy(configuration, endpoint, environment);
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3Client s3Client = s3ClientFactory.create(credentials, configuration);

            Aws.configureServiceClient(s3Client, endpoint, regionName);

            {
                S3ClientOptions.Builder builder = S3ClientOptions.builder();
                if (pathStyleAccess.isPresent()) {
                    builder.setPathStyleAccess(pathStyleAccess.get());
                }
                s3Client.setS3ClientOptions(builder.build());
            }

            GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucket.get(), key.get());

            if (versionId.isPresent()) {
                req.setVersionId(versionId.get());
            }

            Optional<String> sseCustomerKey = s3Secrets.getSecretOptional("sse_c_key");
            if (sseCustomerKey.isPresent()) {
                Optional<String> algorithm = s3Secrets.getSecretOptional("sse_c_key_algorithm");
                Optional<String> md5 = s3Secrets.getSecretOptional("sse_c_key_md5");
                SSECustomerKey sseKey = new SSECustomerKey(sseCustomerKey.get());
                if (algorithm.isPresent()) {
                    sseKey.setAlgorithm(algorithm.get());
                }
                if (md5.isPresent()) {
                    sseKey.setMd5(md5.get());
                }
                req.setSSECustomerKey(sseKey);
            }

            try {
                ObjectMetadata objectMetadata = pollingWaiter(state, "EXISTS")
                        .withPollInterval(POLL_INTERVAL)
                        .withNextIntervalExpBase(NEXT_INTERVAL_EXP_BASE)
                        .withTimeout(timeout)
                        .withWaitMessage("Object '%s/%s' does not yet exist", bucket.get(), key.get())
                        .await(pollState -> pollingRetryExecutor(pollState, "POLL")
                                .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                                .run(s -> {
                                    try {
                                        return Optional.of(s3Client.getObjectMetadata(req));
                                    } catch (AmazonS3Exception e) {
                                        if (e.getStatusCode() == 404) {
                                            return Optional.absent();
                                        }
                                        throw e;
                                    }
                                }));
                return TaskResult.defaultBuilder(request)
                        .resetStoreParams(ImmutableList.of(ConfigKey.of("s3", "last_object")))
                        .storeParams(storeParams(objectMetadata))
                        .build();
            }
            catch (PollingTimeoutException e) {
                logger.debug("s3_wait timed out: {} (making the task {})", (ignoreTimeoutError ? "continued" : "failed"), , e.toString());
                if (ignoreTimeoutError) {
                    return TaskResult.defaultBuilder(request)
                            .resetStoreParams(ImmutableList.of(ConfigKey.of("s3", "last_object")))
                            .build();
                }
                throw e;
            }
        }

        private Config storeParams(ObjectMetadata objectMetadata)
        {
            Config params = request.getConfig().getFactory().create();
            Config object = params.getNestedOrSetEmpty("s3").getNestedOrSetEmpty("last_object");
            object.set("metadata", objectMetadata.getRawMetadata());
            object.set("user_metadata", objectMetadata.getUserMetadata());
            return params;
        }
    }

    interface AmazonS3ClientFactory
    {
        AmazonS3Client create(AWSCredentials credentials, ClientConfiguration clientConfiguration);
    }
}

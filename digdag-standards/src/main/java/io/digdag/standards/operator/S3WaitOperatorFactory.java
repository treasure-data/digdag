package io.digdag.standards.operator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretAccessList;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.Proxies;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.ConfigSelector;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
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
    public SecretAccessList getSecretAccessList()
    {
        return ConfigSelector.builderOfScope("aws")
            .addSecretAccess("region")
            .addSecretAccess("s3.region", "s3.endpoint")
            .addSecretOnlyAccess("access-key-id", "secret-access-key")
            .addSecretOnlyAccess("s3.access-key-id", "s3.secret-access-key", "s3.sse-c-key", "s3.sse-c-key-algorithm", "s3.sse-c-key-md5")
            .build();
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

            Optional<String> endpoint = first(
                    () -> s3Secrets.getSecretOptional("endpoint"),
                    () -> params.getOptional("endpoint", String.class));

            Optional<String> regionName = first(
                    () -> s3Secrets.getSecretOptional("region"),
                    () -> awsSecrets.getSecretOptional("region"),
                    () -> params.getOptional("region", String.class));

            String accessKey = s3Secrets.getSecretOptional("access_key_id")
                    .or(() -> awsSecrets.getSecret("access_key_id"));

            String secretKey = s3Secrets.getSecretOptional("secret_access_key")
                    .or(() -> awsSecrets.getSecret("secret_access_key"));

            // Create S3 Client
            ClientConfiguration configuration = new ClientConfiguration();
            configureProxy(endpoint, configuration);
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3Client s3Client = s3ClientFactory.create(credentials, configuration);

            S3ClientOptions clientOptions = new S3ClientOptions();
            if (pathStyleAccess.isPresent()) {
                clientOptions.setPathStyleAccess(pathStyleAccess.get());
            }
            s3Client.setS3ClientOptions(clientOptions);

            // Configure endpoint or region. Endpoint takes precedence over region.
            if (endpoint.isPresent()) {
                s3Client.setEndpoint(endpoint.get());
            }
            else if (regionName.isPresent()) {
                Regions region;
                try {
                    region = Regions.fromName(regionName.get());
                }
                catch (IllegalArgumentException e) {
                    throw new ConfigException("Illegal AWS region: " + regionName.get());
                }
                s3Client.setRegion(Region.getRegion(region));
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

            ObjectMetadata objectMetadata = pollingWaiter(state, "EXISTS")
                    .withPollInterval(POLL_INTERVAL)
                    .withWaitMessage("Object '%s/%s' does not yet exist", bucket.get(), key.get())
                    .await(pollState -> pollingRetryExecutor(pollState, "POLL")
                            .retryUnless(AmazonServiceException.class, S3WaitOperatorFactory::isDeterministicException)
                            .run(s -> {
                                try {
                                    return Optional.of(s3Client.getObjectMetadata(req));
                                }
                                catch (AmazonS3Exception e) {
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

        private Config storeParams(ObjectMetadata objectMetadata)
        {
            Config params = request.getConfig().getFactory().create();
            Config object = params.getNestedOrSetEmpty("s3").getNestedOrSetEmpty("last_object");
            object.set("metadata", objectMetadata.getRawMetadata());
            object.set("user_metadata", objectMetadata.getUserMetadata());
            return params;
        }
    }

    private static boolean isDeterministicException(AmazonServiceException ex)
    {
        int statusCode = ex.getStatusCode();
        switch (statusCode) {
            case HttpStatus.TOO_MANY_REQUESTS_429:
            case HttpStatus.REQUEST_TIMEOUT_408:
                return false;
            default:
                return statusCode >= 400 && statusCode < 500;
        }
    }

    private void configureProxy(Optional<String> endpoint, ClientConfiguration configuration)
    {
        String scheme = "https";
        if (endpoint.isPresent() && endpoint.get().startsWith("http://")) {
            scheme = "http";
        }
        Optional<ProxyConfig> proxyConfig = Proxies.proxyConfigFromEnv(scheme, environment);
        if (proxyConfig.isPresent()) {
            ProxyConfig cfg = proxyConfig.get();
            configuration.setProxyHost(cfg.getHost());
            configuration.setProxyPort(cfg.getPort());
            Optional<String> user = cfg.getUser();
            if (user.isPresent()) {
                configuration.setProxyUsername(user.get());
            }
            Optional<String> password = cfg.getPassword();
            if (password.isPresent()) {
                configuration.setProxyPassword(password.get());
            }
        }
    }

    @SafeVarargs
    private static <T> Optional<T> first(Supplier<Optional<T>>... suppliers)
    {
        for (Supplier<Optional<T>> supplier : suppliers) {
            Optional<T> optional = supplier.get();
            if (optional.isPresent()) {
                return optional;
            }
        }
        return Optional.absent();
    }

    interface AmazonS3ClientFactory
    {
        AmazonS3Client create(AWSCredentials credentials, ClientConfiguration clientConfiguration);
    }
}

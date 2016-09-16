package io.digdag.standards.operator;

import com.amazonaws.AmazonClientException;
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
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public class S3WaitOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(S3WaitOperatorFactory.class);

    private static final Integer INITIAL_POLL_INTERVAL = 5;
    private static final int MAX_POLL_INTERVAL = 300;

    public String getType()
    {
        return "s3_wait";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new S3WaitOperator(request);
    }

    private class S3WaitOperator
            implements Operator
    {
        private final TaskRequest request;

        public S3WaitOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("aws.*");
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws"));

            Optional<String> command = params.getOptional("_command", String.class);

            Optional<String> bucket = params.getOptional("bucket", String.class);
            Optional<String> key = params.getOptional("key", String.class);
            Optional<String> versionId = params.getOptional("version_id", String.class);

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

            SecretProvider awsSecrets = ctx.secrets().getSecrets("aws");
            Optional<String> endpoint = awsSecrets.getSecretOptional("endpoint").or(params.getOptional("endpoint", String.class));
            Optional<String> regionName = awsSecrets.getSecretOptional("region").or(params.getOptional("region", String.class));
            AWSCredentials credentials = new BasicAWSCredentials(
                    awsSecrets.getSecretOptional("access-key").or(() -> params.get("access-key", String.class)),
                    awsSecrets.getSecret("secret-key"));

            AmazonS3Client s3Client = new AmazonS3Client(credentials);
            s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

            if (endpoint.isPresent()) {
                s3Client.setEndpoint(endpoint.get());
            }
            if (regionName.isPresent()) {

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

            Optional<String> sseCustomerKey = awsSecrets.getSecretOptional("sse-key");
            if (sseCustomerKey.isPresent()) {
                Optional<String> algorithm = awsSecrets.getSecretOptional("sse-key-algorithm");
                Optional<String> md5 = awsSecrets.getSecretOptional("sse-key-md5");
                SSECustomerKey sseKey = new SSECustomerKey(sseCustomerKey.get());
                if (algorithm.isPresent()) {
                    sseKey.setAlgorithm(algorithm.get());
                }
                if (md5.isPresent()) {
                    sseKey.setMd5(md5.get());
                }
                req.setSSECustomerKey(sseKey);
            }

            ObjectMetadata objectMetadata;
            try {
                objectMetadata = s3Client.getObjectMetadata(req);
            }
            catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    // Keep waiting
                    Config state = request.getLastStateParams().deepCopy();
                    int pollIteration = state.get("pollIteration", Integer.class, 0);
                    int pollInterval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, pollIteration), MAX_POLL_INTERVAL);
                    state.set("pollIteration", pollIteration + 1);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }
                else {
                    throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
                }
            }
            catch (AmazonClientException e) {
                throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
            }

            return TaskResult.defaultBuilder(request)
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
}

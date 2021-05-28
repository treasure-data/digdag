package io.digdag.standards.operator.gcp;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.state.TaskState;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

class GcsWaitOperatorFactory
        implements OperatorFactory
{
    private final GcsClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    GcsWaitOperatorFactory(
            GcsClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }

    public String getType()
    {
        return "gcs_wait";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new GcsWaitOperator(context);
    }

    private static final Pattern URI_PATTERN = Pattern.compile("(?:gs://)?(?<bucket>[^/]+)/(?<object>.+)");

    private class GcsWaitOperator
            extends BaseGcsOperator
    {
        private final TaskState state;

        GcsWaitOperator(OperatorContext context)
        {
            super(context, clientFactory, credentialProvider);
            this.state = TaskState.of(request);
        }

        @Override
        protected TaskResult run(GcsClient gcs, String projectId)
        {
            Optional<String> command = params.getOptional("_command", String.class);

            Optional<String> bucket = params.getOptional("bucket", String.class);
            Optional<String> object = params.getOptional("object", String.class);

            //ToDo implement timeout parameter. Please refer to s3_wait>

            if (command.isPresent() && (bucket.isPresent() || object.isPresent()) ||
                    !command.isPresent() && (!bucket.isPresent() || !object.isPresent())) {
                throw new ConfigException("Either the gcs_wait operator command or both 'bucket' and 'object' parameters must be set");
            }

            if (command.isPresent()) {
                Matcher m = URI_PATTERN.matcher(command.get());
                if (!m.matches()) {
                    throw new ConfigException("Illegal GCS URI or path: " + command.get());
                }
                bucket = Optional.of(m.group("bucket"));
                object = Optional.of(m.group("object"));
            }

            return await(gcs, bucket.get(), object.get());
        }

        private TaskResult await(GcsClient gcs, String bucket, String object)
        {
            StorageObject metadata = pollingWaiter(state, "exists")
                    .withWaitMessage("Object '%s/%s' does not yet exist", bucket, object)
                    .await(pollState -> pollingRetryExecutor(pollState, "poll")
                            .retryUnless(GoogleJsonResponseException.class, Gcp::isDeterministicException)
                            .run(s -> gcs.stat(bucket, object)));

            return TaskResult.defaultBuilder(request)
                    .resetStoreParams(ImmutableList.of(ConfigKey.of("gcs_wait", "last_object")))
                    .storeParams(storeParams(metadata))
                    .build();
        }

        private Config storeParams(StorageObject metadata)
        {
            Config params = request.getConfig().getFactory().create();
            Config object = params.getNestedOrSetEmpty("gcs_wait").getNestedOrSetEmpty("last_object");
            object.set("metadata", metadata);
            return params;
        }

        @Override
        public boolean isBlocking()
        {
            return false;
        }
    }
}

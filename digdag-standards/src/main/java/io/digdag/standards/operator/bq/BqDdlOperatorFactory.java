package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.immutables.value.Value;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.bq.Bq.datasetReference;

class BqDdlOperatorFactory
        implements OperatorFactory
{
    private final ObjectMapper objectMapper;
    private final BqJobRunner.Factory bqJobFactory;

    @Inject
    BqDdlOperatorFactory(
            ObjectMapper objectMapper,
            BqJobRunner.Factory bqJobFactory)
    {
        this.objectMapper = objectMapper;
        this.bqJobFactory = bqJobFactory;
    }

    public String getType()
    {
        return "bq_load";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqDdlOperator(projectPath, request);
    }

    private class BqDdlOperator
            extends BaseOperator
    {
        private final Config params;
        private final Config state;
        private final List<BqOperation> operations;

        BqDdlOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
            this.state = request.getLastStateParams().deepCopy();

            this.operations = new ArrayList<>();

            params.getListOrEmpty("delete_datasets", JsonNode.class).stream()
                    .map(this::deleteDataset)
                    .forEach(operations::add);

            params.getListOrEmpty("empty_datasets", JsonNode.class).stream()
                    .map(this::emptyDataset)
                    .forEach(operations::add);

            params.getListOrEmpty("create_datasets", JsonNode.class).stream()
                    .map(this::createDataset)
                    .forEach(operations::add);
        }

        private BqOperation createDataset(JsonNode config)
        {
            return bq -> bq.createDataset(dataset(config));
        }

        private BqOperation emptyDataset(JsonNode config)
        {
            return bq -> bq.emptyDataset(dataset(config));
        }

        private BqOperation deleteDataset(JsonNode config)
        {
            if (!config.isTextual()) {
                throw new ConfigException("Bad dataset reference: " + config);
            }
            DatasetReference datasetReference = datasetReference(config.asText());
            return bq -> {
                String projectId = Optional.fromNullable(datasetReference.getProjectId()).or(bq.projectId());
                bq.deleteDataset(projectId, datasetReference.getDatasetId());
            };
        }

        private Dataset dataset(JsonNode node)
        {
            if (node.isTextual()) {
                return new Dataset()
                        .setDatasetReference(datasetReference(node.asText()));
            }
            else {
                DatasetConfig config;
                try {
                    config = objectMapper.readValue(node.traverse(), DatasetConfig.class);
                }
                catch (IOException e) {
                    throw new ConfigException("Invalid dataset reference or configuration: " + node, e);
                }
                return dataset(config);
            }
        }

        private Dataset dataset(DatasetConfig config)
        {
            return new Dataset()
                    .setDatasetReference(new DatasetReference()
                            .setProjectId(config.project().orNull())
                            .setDatasetId(config.id()))
                    .setFriendlyName(config.friendly_name().orNull())
                    .setDefaultTableExpirationMs(config.default_table_expiration_ms().orNull())
                    .setLocation(config.location().orNull())
                    .setAccess(config.access().orNull())
                    .setLabels(config.labels().orNull());
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("gcp.*");
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            try (BqJobRunner bqJobRunner = bqJobFactory.create(request, ctx)) {
                int operation = state.get("operation", int.class, 0);
                for (int i = operation; i < operations.size(); i++) {
                    state.set("operation", i);
                    BqOperation o = operations.get(i);
                    pollingRetryExecutor(state, "retry")
                            .retryUnless(GoogleJsonResponseException.class, BqDdlOperatorFactory::isDeterministicException)
                            .withErrorMessage("BiqQuery DDL operation failed")
                            .run(() -> o.perform(bqJobRunner));
                }
            }

            return TaskResult.empty(request);
        }
    }

    private static boolean isDeterministicException(GoogleJsonResponseException e)
    {
        return e.getStatusCode() / 100 == 4;
    }

    private interface BqOperation
    {
        void perform(BqJobRunner bqJobRunner)
                throws IOException;
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableDatasetConfig.class)
    interface DatasetConfig
    {
        String id();

        Optional<String> project();

        Optional<String> friendly_name();

        Optional<Long> default_table_expiration_ms();

        Optional<String> location();

        Optional<List<Dataset.Access>> access();

        Optional<Map<String, String>> labels();
    }
}

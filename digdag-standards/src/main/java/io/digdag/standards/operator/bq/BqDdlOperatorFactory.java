package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.api.services.bigquery.model.ViewDefinition;
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
import io.digdag.standards.operator.td.TimestampParam;
import io.digdag.util.BaseOperator;
import io.digdag.util.DurationParam;
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
        private final Optional<DatasetReference> defaultDataset;

        BqDdlOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
            this.state = request.getLastStateParams().deepCopy();
            this.defaultDataset = params.getOptional("dataset", String.class)
                    .transform(Bq::datasetReference);
        }

        private BqOperation createTable(JsonNode jsonNode)
        {
            return bq -> {
                bq.createTable(table(bq.projectId(), defaultDataset, jsonNode));
            };
        }

        private BqOperation emptyTable(JsonNode jsonNode)
        {
            return null;
        }

        private BqOperation deleteTable(JsonNode jsonNode)
        {
            return null;
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
                    .setDefaultTableExpirationMs(config.default_table_expiration().transform(d -> d.getDuration().toMillis()).orNull())
                    .setLocation(config.location().orNull())
                    .setAccess(config.access().orNull())
                    .setLabels(config.labels().orNull());
        }

        private Table table(String defaultProjectId, Optional<DatasetReference> defaultDataset, JsonNode node)
        {
            if (node.isTextual()) {
                return new Table()
                        .setTableReference(Bq.tableReference(defaultProjectId, defaultDataset, node.asText()));
            }
            else {
                TableConfig config;
                try {
                    config = objectMapper.readValue(node.traverse(), TableConfig.class);
                }
                catch (IOException e) {
                    throw new ConfigException("Invalid table reference or configuration: " + node, e);
                }
                return table(defaultProjectId, defaultDataset, config);
            }
        }

        private Table table(String defaultProjectId, Optional<DatasetReference> defaultDataset, TableConfig config)
        {
            Optional<String> datasetId = config.dataset().or(defaultDataset.transform(DatasetReference::getDatasetId));
            if (!datasetId.isPresent()) {
                throw new ConfigException("Bad table reference or configuration: Missing 'dataset'");
            }
            return new Table()
                    .setTableReference(new TableReference()
                            .setProjectId(config.project().or(defaultProjectId))
                            .setDatasetId(datasetId.get())
                            .setTableId(config.id()))
                    .setSchema(config.schema().orNull())
                    .setFriendlyName(config.friendly_name().orNull())
                    .setExpirationTime(config.expiration_time()
                            .transform(p -> p.getTimestamp().toInstant(request.getTimeZone()).toEpochMilli()).orNull())
                    .setTimePartitioning(config.time_partitioning().orNull())
                    .setView(config.view().orNull());
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("gcp.*");
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            List<BqOperation> operations = new ArrayList<>();

            params.getListOrEmpty("delete_datasets", JsonNode.class).stream()
                    .map(this::deleteDataset)
                    .forEach(operations::add);

            params.getListOrEmpty("empty_datasets", JsonNode.class).stream()
                    .map(this::emptyDataset)
                    .forEach(operations::add);

            params.getListOrEmpty("create_datasets", JsonNode.class).stream()
                    .map(this::createDataset)
                    .forEach(operations::add);

            params.getListOrEmpty("delete_tables", JsonNode.class).stream()
                    .map(this::deleteTable)
                    .forEach(operations::add);

            params.getListOrEmpty("empty_tables", JsonNode.class).stream()
                    .map(this::emptyTable)
                    .forEach(operations::add);

            params.getListOrEmpty("create_tables", JsonNode.class).stream()
                    .map(this::createTable)
                    .forEach(operations::add);

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

        Optional<DurationParam> default_table_expiration();

        Optional<String> location();

        Optional<List<Dataset.Access>> access();

        Optional<Map<String, String>> labels();
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableDatasetConfig.class)
    interface TableConfig
    {
        String id();

        Optional<String> project();

        Optional<String> dataset();

        Optional<String> friendly_name();

        Optional<String> description();

        Optional<TimestampParam> expiration_time();

        Optional<TableSchema> schema();

        Optional<TimePartitioning> time_partitioning();

        Optional<ViewDefinition> view();

        Optional<List<Dataset.Access>> access();

        Optional<Map<String, String>> labels();
    }
}

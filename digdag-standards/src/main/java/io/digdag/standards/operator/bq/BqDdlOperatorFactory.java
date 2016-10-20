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
import io.digdag.standards.operator.TimestampParam;
import io.digdag.util.DurationParam;
import org.immutables.value.Value;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.digdag.standards.operator.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.bq.Bq.datasetReference;

class BqDdlOperatorFactory
        implements OperatorFactory
{
    private final ObjectMapper objectMapper;
    private final BqClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    BqDdlOperatorFactory(
            ObjectMapper objectMapper,
            BqClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.objectMapper = objectMapper;
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }

    public String getType()
    {
        return "bq_ddl";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqDdlOperator(projectPath, request);
    }

    private class BqDdlOperator
            extends BaseBqOperator
    {
        private final Config state;
        private final Optional<DatasetReference> defaultDataset;

        BqDdlOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, clientFactory, credentialProvider);
            this.state = request.getLastStateParams().deepCopy();
            this.defaultDataset = params.getOptional("dataset", String.class)
                    .transform(Bq::datasetReference);
        }

        private BqOperation createTable(JsonNode config)
        {
            return (bq, projectId) -> bq.createTable(projectId, table(projectId, defaultDataset, config));
        }

        private BqOperation emptyTable(JsonNode config)
        {
            return (bq, projectId) -> bq.emptyTable(projectId, table(projectId, defaultDataset, config));
        }

        private BqOperation deleteTable(JsonNode config)
        {
            if (!config.isTextual()) {
                throw new ConfigException("Bad table reference: " + config);
            }
            return (bq, projectId) -> {
                TableReference r = Bq.tableReference(projectId, defaultDataset, config.asText());
                bq.deleteTable(r.getProjectId(), r.getDatasetId(), r.getTableId());
            };
        }

        private BqOperation createDataset(JsonNode config)
        {
            return (bq, projectId) -> bq.createDataset(projectId, dataset(projectId, config));
        }

        private BqOperation emptyDataset(JsonNode config)
        {
            return (bq, projectId) -> bq.emptyDataset(projectId, dataset(projectId, config));
        }

        private BqOperation deleteDataset(JsonNode config)
        {
            if (!config.isTextual()) {
                throw new ConfigException("Bad dataset reference: " + config);
            }
            return (bq, projectId) -> {
                DatasetReference r = datasetReference(projectId, config.asText());
                bq.deleteDataset(r.getProjectId(), r.getDatasetId());
            };
        }

        private Dataset dataset(String defaultProjectId, JsonNode node)
        {
            if (node.isTextual()) {
                return new Dataset()
                        .setDatasetReference(datasetReference(defaultProjectId, node.asText()));
            }
            else {
                DatasetConfig config;
                try {
                    config = objectMapper.readValue(node.traverse(), DatasetConfig.class);
                }
                catch (IOException e) {
                    throw new ConfigException("Invalid dataset reference or configuration: " + node, e);
                }
                return dataset(defaultProjectId, config);
            }
        }

        private Dataset dataset(String defaultProjectId, DatasetConfig config)
        {
            return new Dataset()
                    .setDatasetReference(new DatasetReference()
                            .setProjectId(config.project().or(defaultProjectId))
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
//                    .setTimePartitioning(config.time_partitioning().orNull())
                    .setView(config.view().orNull());
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("gcp.*");
        }

        @Override
        protected TaskResult run(TaskExecutionContext ctx, BqClient bq, String projectId)

        {
            List<BqOperation> operations = Stream.of(
                    params.getListOrEmpty("delete_datasets", JsonNode.class).stream().map(this::deleteDataset),
                    params.getListOrEmpty("empty_datasets", JsonNode.class).stream().map(this::emptyDataset),
                    params.getListOrEmpty("create_datasets", JsonNode.class).stream().map(this::createDataset),
                    params.getListOrEmpty("delete_tables", JsonNode.class).stream().map(this::deleteTable),
                    params.getListOrEmpty("empty_tables", JsonNode.class).stream().map(this::emptyTable),
                    params.getListOrEmpty("create_tables", JsonNode.class).stream().map(this::createTable))
                    .flatMap(s -> s)
                    .collect(Collectors.toList());

            int operation = state.get("operation", int.class, 0);
            for (int i = operation; i < operations.size(); i++) {
                state.set("operation", i);
                BqOperation o = operations.get(i);
                pollingRetryExecutor(state, state, "retry")
                        .retryUnless(GoogleJsonResponseException.class, BqDdlOperatorFactory::isDeterministicException)
                        .withErrorMessage("BiqQuery DDL operation failed")
                        .run(() -> o.perform(bq, projectId));
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
        void perform(BqClient bq, String projectId)
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
    @JsonDeserialize(as = ImmutableTableConfig.class)
    interface TableConfig
    {
        String id();

        Optional<String> project();

        Optional<String> dataset();

        Optional<String> friendly_name();

        Optional<String> description();

        // TODO: parse local / human-friendly timestamps
        Optional<TimestampParam> expiration_time();

        Optional<TableSchema> schema();

        // TODO
//        Optional<TimePartitioning> time_partitioning();

        Optional<ViewDefinition> view();

        Optional<List<Dataset.Access>> access();

        Optional<Map<String, String>> labels();
    }
}

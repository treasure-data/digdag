package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.td.YamlLoader;
import io.digdag.util.BaseOperator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static io.digdag.standards.operator.bq.Bq.tableReference;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

class BqLoadOperatorFactory
        implements OperatorFactory
{
    private final ObjectMapper objectMapper;
    private final TemplateEngine templateEngine;
    private final BqJobRunner.Factory bqJobFactory;

    @Inject
    public BqLoadOperatorFactory(
            ObjectMapper objectMapper,
            TemplateEngine templateEngine,
            BqJobRunner.Factory bqJobFactory)
    {
        this.objectMapper = objectMapper;
        this.templateEngine = templateEngine;
        this.bqJobFactory = bqJobFactory;
    }

    public String getType()
    {
        return "bq_load";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqLoadOperator(projectPath, request);
    }

    private class BqLoadOperator
            extends BaseOperator
    {
        private final Config params;
        private final List<String> sourceUris;

        BqLoadOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));

            this.sourceUris = sourceUris(params);
        }

        private List<String> sourceUris(Config params)
        {
            try {
                return params.getList("_command", String.class);
            }
            catch (ConfigException ignore) {
                return ImmutableList.of(params.get("_command", String.class));
            }
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
                return result(bqJobRunner.runJob(loadJobConfig(bqJobRunner.projectId())));
            }
        }

        private JobConfiguration loadJobConfig(String projectId)
        {
            JobConfigurationLoad cfg = new JobConfigurationLoad()
                    .setSourceUris(sourceUris);

            if (params.has("schema")) {
                cfg.setSchema(tableSchema(params));
            }

            Optional<String> defaultDataset = params.getOptional("dataset", String.class);

            String destinationTable = params.get("destination_table", String.class);
            cfg.setDestinationTable(tableReference(projectId, defaultDataset, destinationTable));

            params.getOptional("create_disposition", String.class).transform(cfg::setCreateDisposition);
            params.getOptional("write_disposition", String.class).transform(cfg::setWriteDisposition);

            params.getOptional("source_format", String.class).transform(cfg::setSourceFormat);
            params.getOptional("field_delimiter", String.class).transform(cfg::setFieldDelimiter);
            params.getOptional("skip_leading_rows", int.class).transform(cfg::setSkipLeadingRows);
            params.getOptional("encoding", String.class).transform(cfg::setEncoding);
            params.getOptional("quote", String.class).transform(cfg::setQuote);
            params.getOptional("max_bad_records", int.class).transform(cfg::setMaxBadRecords);
            params.getOptional("allow_quoted_newlines", boolean.class).transform(cfg::setAllowQuotedNewlines);
            params.getOptional("allow_jagged_rows", boolean.class).transform(cfg::setAllowJaggedRows);
            params.getOptional("ignore_unknown_values", boolean.class).transform(cfg::setIgnoreUnknownValues);
            params.getOptional("projection_fields", new TypeReference<List<String>>() {}).transform(cfg::setProjectionFields);
            params.getOptional("autodetect", boolean.class).transform(cfg::setAutodetect);
            params.getOptional("schema_update_options", new TypeReference<List<String>>() {}).transform(cfg::setSchemaUpdateOptions);

            return new JobConfiguration()
                    .setLoad(cfg);
        }

        private TableSchema tableSchema(Config params)
        {
            try {
                return params.get("schema", TableSchema.class);
            }
            catch (ConfigException ignore) {
            }

            String fileName = params.get("schema", String.class);
            try {

                String schemaYaml = workspace.templateFile(templateEngine, fileName, UTF_8, params);
                ObjectNode schemaJson = new YamlLoader().loadString(schemaYaml);
                return objectMapper.readValue(schemaJson.traverse(), TableSchema.class);
            }
            catch (IOException ex) {
                throw workspace.propagateIoException(ex, fileName, ConfigException::new);
            }
            catch (TemplateException ex) {
                throw new ConfigException(
                        String.format(ENGLISH, "%s in %s", ex.getMessage(), fileName),
                        ex);
            }
        }

        private TaskResult result(Job job)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config bq_load = result.getNestedOrSetEmpty("bq_load");
            bq_load.set("last_jobid", job.getId());
            return TaskResult.defaultBuilder(request)
                    .storeParams(result)
                    .addResetStoreParams(ConfigKey.of("bq_load", "last_jobid"))
                    .build();
        }
    }
}

package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.HivePartitioningOptions;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.ParquetOptions;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.td.YamlLoader;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import static io.digdag.standards.operator.gcp.Bq.tableReference;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

class BqLoadOperatorFactory
        implements OperatorFactory
{
    private final ObjectMapper objectMapper;
    private final TemplateEngine templateEngine;
    private final BqClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    public BqLoadOperatorFactory(
            ObjectMapper objectMapper,
            TemplateEngine templateEngine,
            BqClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.objectMapper = objectMapper;
        this.templateEngine = templateEngine;
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }

    public String getType()
    {
        return "bq_load";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new BqLoadOperator(context);
    }

    private class BqLoadOperator
            extends BaseBqJobOperator
    {
        BqLoadOperator(OperatorContext context)
        {
            super(context, clientFactory, credentialProvider);
        }

        @Override
        protected JobConfiguration jobConfiguration(String projectId)
        {
            JobConfigurationLoad cfg = new JobConfigurationLoad()
                    .setSourceUris(sourceUris(params));

            if (params.has("schema")) {
                cfg.setSchema(tableSchema(params));
            }

            Optional<DatasetReference> defaultDataset = params.getOptional("dataset", String.class)
                    .transform(Bq::datasetReference);

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
            Optional.of(params.getListOrEmpty("projection_fields", String.class)).transform(cfg::setProjectionFields);
            params.getOptional("autodetect", boolean.class).transform(cfg::setAutodetect);
            Optional.of(params.getListOrEmpty("schema_update_options", String.class)).transform(cfg::setSchemaUpdateOptions);
            params.getOptional("clustering", Clustering.class).transform(cfg::setClustering);
            Optional.of(params.getListOrEmpty("decimal_target_types", String.class)).transform(cfg::setDecimalTargetTypes);
            params.getOptional("encryption_configuration", EncryptionConfiguration.class).transform(cfg::setDestinationEncryptionConfiguration);
            params.getOptional("hive_partitioning_options", HivePartitioningOptions.class).transform(cfg::setHivePartitioningOptions);
            params.getOptional("json_extension", String.class).transform(cfg::setJsonExtension);
            params.getOptional("null_marker", String.class).transform(cfg::setNullMarker);
            params.getOptional("parquet_options", ParquetOptions.class).transform(cfg::setParquetOptions);
            params.getOptional("use_avro_logical_types", boolean.class).transform(cfg::setUseAvroLogicalTypes);

            if (params.has("range_partitioning")) {
                cfg.setRangePartitioning(rangePartitioning(params.getNested("range_partitioning")));
            }

            if (params.has("time_partitioning")) {
                cfg.setTimePartitioning(timePartitioning(params.getNested("time_partitioning")));
            }

            return new JobConfiguration()
                    .setLoad(cfg);
        }

        private List<String> sourceUris(Config params)
        {
            try {
                return params.parseList("_command", String.class);
            }
            catch (ConfigException ignore) {
                return ImmutableList.of(params.get("_command", String.class));
            }
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
                String schemaString = workspace.templateFile(templateEngine, fileName, UTF_8, params);
                if (FilenameUtils.getExtension(fileName).equals("json")) {
                    return objectMapper.readValue(schemaString, TableSchema.class);
                }
                else {
                    ObjectNode schemaJson = new YamlLoader().loadString(schemaString);
                    return objectMapper.readValue(schemaJson.traverse(), TableSchema.class);
                }
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
    }
}

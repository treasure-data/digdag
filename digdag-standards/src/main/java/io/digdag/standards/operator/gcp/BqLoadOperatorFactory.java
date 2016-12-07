package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretAccessList;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.util.ConfigSelector;
import io.digdag.standards.operator.td.YamlLoader;

import java.io.IOException;
import java.util.List;

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

    @Override
    public SecretAccessList getSecretAccessList()
    {
        return ConfigSelector.builderOfScope("gcp")
            .addSecretAccess("project")
            .addSecretOnlyAccess("credential")
            .build();
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
            params.getOptional("projection_fields", new TypeReference<List<String>>() {}).transform(cfg::setProjectionFields);
            params.getOptional("autodetect", boolean.class).transform(cfg::setAutodetect);
            params.getOptional("schema_update_options", new TypeReference<List<String>>() {}).transform(cfg::setSchemaUpdateOptions);

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
    }
}

package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.UserDefinedFunctionResource;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TemplateEngine;

import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.gcp.Bq.tableReference;
import static java.nio.charset.StandardCharsets.UTF_8;

class BqOperatorFactory
        implements OperatorFactory
{
    private final TemplateEngine templateEngine;
    private final BqClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    public BqOperatorFactory(
            TemplateEngine templateEngine,
            BqClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.templateEngine = templateEngine;
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }

    public String getType()
    {
        return "bq";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new BqOperator(context);
    }

    private class BqOperator
            extends BaseBqJobOperator
    {
        private final String query;

        BqOperator(OperatorContext context)
        {
            super(context, clientFactory, credentialProvider);
            this.query = workspace.templateCommand(templateEngine, params, "query", UTF_8);
        }

        @Override
        protected JobConfiguration jobConfiguration(String projectId)
        {
            JobConfigurationQuery cfg = new JobConfigurationQuery()
                    .setQuery(query);

            cfg.setUseLegacySql(params.get("use_legacy_sql", boolean.class, false));

            params.getOptional("allow_large_results", boolean.class).transform(cfg::setAllowLargeResults);
            params.getOptional("use_query_cache", Boolean.class).transform(cfg::setUseQueryCache);
            params.getOptional("create_disposition", String.class).transform(cfg::setCreateDisposition);
            params.getOptional("write_disposition", String.class).transform(cfg::setWriteDisposition);
            params.getOptional("flatten_results", Boolean.class).transform(cfg::setFlattenResults);
            params.getOptional("maximum_billing_tier", Integer.class).transform(cfg::setMaximumBillingTier);
            params.getOptional("priority", String.class).transform(cfg::setPriority);

            params.getOptional("table_definitions", new TypeReference<Map<String, ExternalDataConfiguration>>() {})
                    .transform(cfg::setTableDefinitions);
            params.getOptional("user_defined_function_resources", new TypeReference<List<UserDefinedFunctionResource>>() {})
                    .transform(cfg::setUserDefinedFunctionResources);

            Optional<DatasetReference> defaultDataset = params.getOptional("dataset", String.class)
                    .transform(Bq::datasetReference);
            defaultDataset.transform(cfg::setDefaultDataset);

            params.getOptional("destination_table", String.class)
                    .transform(s -> cfg.setDestinationTable(tableReference(projectId, defaultDataset, s)));
            params.getOptional("clustering", Clustering.class).transform(cfg::setClustering);
            params.getOptional("encryption_configuration", EncryptionConfiguration.class).transform(cfg::setDestinationEncryptionConfiguration);
            params.getOptional("maximum_bytes_billed", Long.class).transform(cfg::setMaximumBytesBilled);
            Optional.of(params.getListOrEmpty("schema_update_options", String.class)).transform(cfg::setSchemaUpdateOptions);

            if (params.has("range_partitioning")) {
                cfg.setRangePartitioning(rangePartitioning(params.getNested("range_partitioning")));
            }

            if (params.has("time_partitioning")) {
                cfg.setTimePartitioning(timePartitioning(params.getNested("time_partitioning")));
            }

            return new JobConfiguration()
                    .setQuery(cfg);
        }
    }
}

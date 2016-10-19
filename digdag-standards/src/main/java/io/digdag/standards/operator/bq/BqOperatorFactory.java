package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.UserDefinedFunctionResource;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.digdag.standards.operator.bq.Bq.datasetReference;
import static io.digdag.standards.operator.bq.Bq.tableReference;
import static java.nio.charset.StandardCharsets.UTF_8;

class BqOperatorFactory
        implements OperatorFactory
{
    private final TemplateEngine templateEngine;
    private final BqJobRunner.Factory bqJobFactory;

    @Inject
    public BqOperatorFactory(
            TemplateEngine templateEngine,
            BqJobRunner.Factory bqJobFactory)
    {
        this.templateEngine = templateEngine;
        this.bqJobFactory = bqJobFactory;
    }

    public String getType()
    {
        return "bq";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqOperator(projectPath, request);
    }

    private class BqOperator
            extends BaseOperator
    {
        private final Config params;
        private final String query;

        BqOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
            this.query = workspace.templateCommand(templateEngine, params, "query", UTF_8);
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
                return result(bqJobRunner.runJob(queryJobConfig(bqJobRunner.projectId())));
            }
        }

        private JobConfiguration queryJobConfig(String projectId)
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

            return new JobConfiguration()
                    .setQuery(cfg);
        }

        private TaskResult result(Job job)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config bq = result.getNestedOrSetEmpty("bq");
            bq.set("last_jobid", job.getId());
            return TaskResult.defaultBuilder(request)
                    .storeParams(result)
                    .addResetStoreParams(ConfigKey.of("bq", "last_jobid"))
                    .build();
        }
    }
}

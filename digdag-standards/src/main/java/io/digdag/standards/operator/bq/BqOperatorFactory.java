package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.digdag.standards.operator.bq.Bq.datasetReference;
import static io.digdag.standards.operator.bq.Bq.tableReference;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BqOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(BqOperatorFactory.class);

    private final TemplateEngine templateEngine;
    private final ObjectMapper objectMapper;
    private final BqJobRunner.Factory bqJobFactory;

    private final Map<String, String> environment;

    @Inject
    public BqOperatorFactory(
            @Environment Map<String, String> environment,
            TemplateEngine templateEngine,
            ObjectMapper objectMapper,
            BqJobRunner.Factory bqJobFactory)
    {
        this.environment = environment;
        this.templateEngine = templateEngine;
        this.objectMapper = objectMapper;
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
//            try (BqJobRunner bqJob = new BqJobRunner(request, ctx, objectMapper, environment)) {
            try (BqJobRunner bqJobRunner = bqJobFactory.create(request, ctx)) {
                return result(bqJobRunner.runJob(queryJobConfig(bqJobRunner.projectId())));
            }
        }

        private JobConfiguration queryJobConfig(String projectId)
        {
            // TODO: This could perhaps just be: params.get("config", JobConfigurationQuery.class);
            //       Then we could just refer to the BigQuery documentation for the config param

            JobConfigurationQuery queryConfig = new JobConfigurationQuery()
                    .setQuery(query);

            configure(params, "allow_large_results", Boolean.class, queryConfig::setAllowLargeResults);
            configure(params, "use_legacy_sql", Boolean.class, queryConfig::setUseLegacySql);
            configure(params, "use_query_cache", Boolean.class, queryConfig::setUseQueryCache);
            configure(params, "create_disposition", String.class, queryConfig::setCreateDisposition);
            configure(params, "write_disposition", String.class, queryConfig::setWriteDisposition);
            configure(params, "flatten_results", Boolean.class, queryConfig::setFlattenResults);
            configure(params, "maximum_billing_tier", Integer.class, queryConfig::setMaximumBillingTier);
            configure(params, "preserve_nulls", Boolean.class, queryConfig::setPreserveNulls);
            configure(params, "priority", String.class, queryConfig::setPriority);
            configure(params, "table_definitions", new TypeReference<Map<String, ExternalDataConfiguration>>() {}, queryConfig::setTableDefinitions);
            configure(params, "user_defined_function_resources", new TypeReference<List<UserDefinedFunctionResource>>() {}, queryConfig::setUserDefinedFunctionResources);

            Optional<String> defaultDataset = params.getOptional("default_dataset", String.class);
            if (defaultDataset.isPresent()) {
                queryConfig.setDefaultDataset(datasetReference(defaultDataset.get()));
            }
            Optional<String> destinationTable = params.getOptional("destination_table", String.class);
            if (destinationTable.isPresent()) {
                queryConfig.setDestinationTable(tableReference(projectId, defaultDataset, destinationTable.get()));
            }

            return new JobConfiguration()
                    .setQuery(queryConfig);
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

    private static <T> void configure(Config params, String key, Class<T> cls, Consumer<T> action)
    {
        Optional<T> value = params.getOptional(key, cls);
        if (value.isPresent()) {
            action.accept(value.get());
        }
    }

    private static <T> void configure(Config params, String key, TypeReference<T> type, Consumer<T> action)
    {
        Optional<T> value = params.getOptional(key, type);
        if (value.isPresent()) {
            action.accept(value.get());
        }
    }
}

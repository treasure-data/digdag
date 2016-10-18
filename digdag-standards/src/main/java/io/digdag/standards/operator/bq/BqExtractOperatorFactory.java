package io.digdag.standards.operator.bq;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
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
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.util.List;

import static io.digdag.standards.operator.bq.Bq.tableReference;

class BqExtractOperatorFactory
        implements OperatorFactory
{
    private final TemplateEngine templateEngine;
    private final BqJobRunner.Factory bqJobFactory;

    @Inject
    public BqExtractOperatorFactory(
            TemplateEngine templateEngine,
            BqJobRunner.Factory bqJobFactory)
    {
        this.templateEngine = templateEngine;
        this.bqJobFactory = bqJobFactory;
    }

    public String getType()
    {
        return "bq_extract";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqExtractOperator(projectPath, request);
    }

    private class BqExtractOperator
            extends BaseOperator
    {
        private final Config params;

        BqExtractOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
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
                return result(bqJobRunner.runJob(extractJobConfig(bqJobRunner.projectId())));
            }
        }

        private JobConfiguration extractJobConfig(String projectId)
        {
            JobConfigurationExtract cfg = new JobConfigurationExtract();

            try {
                cfg.setDestinationUris(params.getList("destination", String.class));
            }
            catch (ConfigException ignore) {
                cfg.setDestinationUri(params.get("destination", String.class));
            }

            Optional<String> defaultDataset = params.getOptional("dataset", String.class);
            String sourceTable = params.get("_command", String.class);
            cfg.setSourceTable(tableReference(projectId, defaultDataset, sourceTable));

            params.getOptional("print_header", boolean.class).transform(cfg::setPrintHeader);
            params.getOptional("field_delimiter", String.class).transform(cfg::setFieldDelimiter);
            params.getOptional("destination_format", String.class).transform(cfg::setDestinationFormat);
            params.getOptional("compression", String.class).transform(cfg::setCompression);

            return new JobConfiguration()
                    .setExtract(cfg);
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

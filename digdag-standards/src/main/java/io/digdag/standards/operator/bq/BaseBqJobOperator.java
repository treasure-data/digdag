package io.digdag.standards.operator.bq;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseBqJobOperator
        extends BaseBqOperator
{
    BaseBqJobOperator(Path projectPath, TaskRequest request, BqClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request, clientFactory, credentialProvider);
    }

    @Override
    protected TaskResult run(TaskExecutionContext ctx, BqClient bq, String projectId)
    {
        BqJobRunner jobRunner = new BqJobRunner(request, bq, projectId);
        Job completed = jobRunner.runJob(jobConfiguration(projectId));
        return result(completed);
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

    protected abstract JobConfiguration jobConfiguration(String projectId);
}

package io.digdag.standards.operator.gcp;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.RangePartitioning;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseBqJobOperator
        extends BaseBqOperator
{
    BaseBqJobOperator(OperatorContext context, BqClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(context, clientFactory, credentialProvider);
    }

    @Override
    protected TaskResult run(BqClient bq, String projectId)
    {
        BqJobRunner jobRunner = new BqJobRunner(request, bq, projectId);
        Optional<String> location = params.getOptional("location", String.class);
        Job completed = jobRunner.runJob(jobConfiguration(projectId), location);
        return result(completed);
    }

    private TaskResult result(Job job)
    {
        ConfigFactory cf = request.getConfig().getFactory();
        Config result = cf.create();
        Config bq = result.getNestedOrSetEmpty("bq");
        bq.set("last_job_id", job.getId());
        bq.set("last_jobid", job.getId());
        return TaskResult.defaultBuilder(request)
                .storeParams(result)
                .addResetStoreParams(ConfigKey.of("bq", "last_job_id"))
                .addResetStoreParams(ConfigKey.of("bq", "last_jobid"))
                .build();
    }

    protected abstract JobConfiguration jobConfiguration(String projectId);

    protected RangePartitioning rangePartitioning(Config params) {
        Config rangeParams = params.getNested("range");
        RangePartitioning.Range range = new RangePartitioning.Range();
        range.setStart(rangeParams.get("start", Long.class))
                .setEnd(rangeParams.get("end", Long.class))
                .setInterval(rangeParams.get("interval", Long.class));

        RangePartitioning rPart = new RangePartitioning();
        rPart.setField(params.get("field", String.class))
                .setRange(range);

        return rPart;
    }

    protected TimePartitioning timePartitioning(Config params) {
        TimePartitioning tPart = new TimePartitioning();
        // required fields
        tPart.setType(params.get("type", String.class));

        // optional fields
        params.getOptional("field", String.class).transform(tPart::setField);
        params.getOptional("requirePartitionFilter", Boolean.class).transform(tPart::setRequirePartitionFilter);
        if (params.has("expirationMs")) {
            tPart.setExpirationMs(params.get("expirationMs", Long.class));
        }

        return tPart;
    }
}

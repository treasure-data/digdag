package io.digdag.standards.operator.gcp;

import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseGcsOperator
        extends BaseGcpOperator
{
    private final GcsClient.Factory clientFactory;

    protected final Config params;

    protected BaseGcsOperator(OperatorContext context, GcsClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(context, credentialProvider);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("gcs"));
    }

    @Override
    protected TaskResult run(GcpCredential credential, String projectId)
    {
        try (GcsClient bq = clientFactory.create(credential.credential())) {
            return run(bq, projectId);
        }
    }

    protected abstract TaskResult run(GcsClient gcs, String projectId);
}

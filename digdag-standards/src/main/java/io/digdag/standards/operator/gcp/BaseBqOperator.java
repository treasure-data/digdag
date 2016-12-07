package io.digdag.standards.operator.gcp;

import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseBqOperator
        extends BaseGcpOperator
{
    private final BqClient.Factory clientFactory;

    protected final Config params;

    protected BaseBqOperator(OperatorContext context, BqClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(context, credentialProvider);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
    }

    @Override
    protected TaskResult run(GcpCredential credential, String projectId)
    {
        try (BqClient bq = clientFactory.create(credential.credential())) {
            return run(bq, projectId);
        }
    }

    protected abstract TaskResult run(BqClient bq, String projectId);
}

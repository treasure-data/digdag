package io.digdag.standards.operator.gcp;

import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseBqOperator
        extends BaseGcpOperator
{
    private final BqClient.Factory clientFactory;

    protected final Config params;

    protected BaseBqOperator(Path projectPath, TaskRequest request, BqClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request, credentialProvider);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
    }

    @Override
    protected TaskResult run(TaskExecutionContext ctx, GcpCredential credential, String projectId)
    {
        try (BqClient bq = clientFactory.create(credential.credential())) {
            return run(ctx, bq, projectId);
        }
    }

    protected abstract TaskResult run(TaskExecutionContext ctx, BqClient bq, String projectId);
}

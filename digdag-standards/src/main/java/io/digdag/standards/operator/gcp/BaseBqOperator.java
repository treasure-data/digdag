package io.digdag.standards.operator.gcp;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.util.List;

abstract class BaseBqOperator
        extends BaseOperator
{
    private final BqClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    protected final Config params;

    protected BaseBqOperator(Path projectPath, TaskRequest request, BqClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));

        this.credentialProvider = credentialProvider;
    }

    @Override
    public List<String> secretSelectors()
    {
        return ImmutableList.of("gcp.*");
    }

    @Override
    public TaskResult runTask(TaskExecutionContext ctx)
    {
        GcpCredential credential = credentialProvider.credential(ctx.secrets());
        String projectId = projectId(ctx, credential);
        try (BqClient bq = clientFactory.create(credential.credential())) {
            return run(ctx, bq, projectId);
        }
    }

    private String projectId(TaskExecutionContext ctx, GcpCredential credential)
    {
        Optional<String> projectId = ctx.secrets().getSecretOptional("gcp.project")
                .or(credential.projectId());
        if (!projectId.isPresent()) {
            throw new TaskExecutionException("Missing 'gcp.project' secret", ConfigElement.empty());
        }

        return projectId.get();
    }

    protected abstract TaskResult run(TaskExecutionContext ctx, BqClient bq, String projectId);
}

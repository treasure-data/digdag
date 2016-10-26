package io.digdag.standards.operator.gcp;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

import java.nio.file.Path;
import java.util.List;

abstract class BaseGcpOperator
        extends BaseOperator
{
    private final GcpCredentialProvider credentialProvider;

    protected BaseGcpOperator(Path projectPath, TaskRequest request, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request);
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
        return run(ctx, credential, projectId);
    }

    protected abstract TaskResult run(TaskExecutionContext ctx, GcpCredential credential, String projectId);

    private String projectId(TaskExecutionContext ctx, GcpCredential credential)
    {
        Optional<String> projectId = ctx.secrets().getSecretOptional("gcp.project")
                .or(credential.projectId());
        if (!projectId.isPresent()) {
            throw new TaskExecutionException("Missing 'gcp.project' secret", ConfigElement.empty());
        }

        return projectId.get();
    }
}

package io.digdag.standards.operator.gcp;

import com.google.common.base.Optional;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

abstract class BaseGcpOperator
        extends BaseOperator
{
    private final GcpCredentialProvider credentialProvider;

    protected BaseGcpOperator(OperatorContext context, GcpCredentialProvider credentialProvider)
    {
        super(context);
        this.credentialProvider = credentialProvider;
    }

    @Override
    public TaskResult runTask()
    {
        GcpCredential credential = credentialProvider.credential(context.getSecrets());
        String projectId = projectId(credential);
        return run(credential, projectId);
    }

    protected abstract TaskResult run(GcpCredential credential, String projectId);

    private String projectId(GcpCredential credential)
    {
        Optional<String> projectId = request.getConfig().getNestedOrGetEmpty("gcp")
                .getOptional("project", String.class)
                .or(context.getSecrets().getSecretOptional("gcp.project"))
                .or(credential.projectId());
        if (!projectId.isPresent()) {
            throw new TaskExecutionException("Missing 'gcp.project' secret");
        }

        return projectId.get();
    }
}

package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
        extends Target
{
    int getSiteId();

    String getName();

    int getProjectId();

    String getProjectName();

    long getWorkflowId();

    String getWorkflowName();

    int getRevisionId();

    String getRevisionName();

    static AttemptTarget of(
            int siteId,
            String name,
            int projectId,
            String projectName,
            long workflowId,
            String workflowName,
            int revisionId,
            String revisionName)
    {
        return ImmutableAttemptTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectId(projectId)
                .projectName(projectName)
                .workflowId(workflowId)
                .workflowName(workflowName)
                .revisionId(revisionId)
                .revisionName(revisionName)
                .build();
    }
}

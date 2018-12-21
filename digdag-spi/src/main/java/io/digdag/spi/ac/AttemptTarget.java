package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
{
    int getSiteId();

    Optional<String> getName();

    int getProjectId();

    String getProjectName();

    long getWorkflowId();

    String getWorkflowName();

    int getRevisionId();

    String getRevisionName();

    static AttemptTarget of(
            int siteId,
            Optional<String> name,
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

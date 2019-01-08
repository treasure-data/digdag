package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
        extends Target
{
    int getSiteId();

    Optional<String> getName();

    int getProjectId();

    String getProjectName();

    String getWorkflowName();

    static AttemptTarget of(
            int siteId,
            Optional<String> name,
            int projectId,
            String projectName,
            String workflowName)
    {
        return ImmutableAttemptTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectId(projectId)
                .projectName(projectName)
                .workflowName(workflowName)
                .build();
    }
}

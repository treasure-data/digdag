package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface WorkflowTarget
{
    int getSiteId();

    String getName();

    Optional<Integer> getProjectId();

    String getProjectName();

    static WorkflowTarget of(int siteId, String name, Optional<Integer> projectId, String projectName)
    {
        return ImmutableWorkflowTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectId(projectId)
                 .projectName(projectName)
                .build();
    }
}

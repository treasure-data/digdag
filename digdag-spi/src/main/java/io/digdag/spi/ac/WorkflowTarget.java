package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface WorkflowTarget
{
    int getSiteId();

    String getName();

    int getProjectId();

    String getProjectName();

    public static WorkflowTarget of(int siteId, String name, int projectId, String projectName)
    {
        return ImmutableWorkflowTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }
}

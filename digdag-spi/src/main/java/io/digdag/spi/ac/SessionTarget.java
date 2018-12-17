package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface SessionTarget
{
    int getSiteId();

    int getId();

    String getWorkflowName();

    int getProjectId();

    String getProjectName();

    public static SessionTarget of(int siteId, int id, String workflowName, int projectId, String projectName)
    {
        return ImmutableSessionTarget.builder()
                .siteId(siteId)
                .id(id)
                .workflowName(workflowName)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }
}

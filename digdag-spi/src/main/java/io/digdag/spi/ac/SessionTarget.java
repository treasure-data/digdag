package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface SessionTarget
        extends Target
{
    int getSiteId();

    long getId();

    String getWorkflowName();

    String getProjectName();

    static SessionTarget of(int siteId, long id, String workflowName, String projectName)
    {
        return ImmutableSessionTarget.builder()
                .siteId(siteId)
                .id(id)
                .workflowName(workflowName)
                .projectName(projectName)
                .build();
    }
}

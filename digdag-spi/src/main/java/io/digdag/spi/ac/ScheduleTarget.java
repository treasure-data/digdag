package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface ScheduleTarget
{
    int getSiteId();

    String getProjectName();

    String getWorkflowName();

    int getId();

    static ScheduleTarget of(int siteId, String projectName, String workflowName, int id)
    {
        return ImmutableScheduleTarget.builder()
                .siteId(siteId)
                .projectName(projectName)
                .workflowName(workflowName)
                .id(id)
                .build();
    }
}

package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
{
    int getSiteId();

    String getProjectName();

    String getWorkflowName();

    int getSessionId();

    int getId();

    static AttemptTarget of(int siteId, String projectName, String workflowName, int sessionId, int id)
    {
        return ImmutableAttemptTarget.builder()
                .siteId(siteId)
                .projectName(projectName)
                .workflowName(workflowName)
                .sessionId(sessionId)
                .id(id)
                .build();
    }
}

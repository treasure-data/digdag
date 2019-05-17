package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
{
    int getSiteId();

    String getProjectName();

    String getWorkflowName();

    long getSessionId();

    long getId();

    static AttemptTarget of(int siteId, String projectName, String workflowName, long sessionId, long id)
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

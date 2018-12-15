package io.digdag.spi;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import java.util.List;
import org.immutables.value.Value;

public interface AccessController
{
    @Value.Immutable
    interface ProjectTarget
    {
        int getId();
        String getName();
    }

    static ProjectTarget buildProjectTarget(int id, String name)
    {
        return ImmutableProjectTarget.builder()
                .id(id)
                .name(name)
                .build();
    }

    @Value.Immutable
    interface WorkflowTarget
    {
        String getName();
        int getProjectId();
        String getProjectName();
    }

    static WorkflowTarget buildWorkflowTarget(String name, int projectId, String projectName)
    {
        return ImmutableWorkflowTarget.builder()
                .name(name)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }

    @Value.Immutable
    interface SessionTarget
    {
        int getId();
        String getWorkflowName();
        int getProjectId();
        String getProjectName();
    }

    static SessionTarget buildSessionTarget(int id, String workflowName, int projectId, String projectName)
    {
        return ImmutableSessionTarget.builder()
                .id(id)
                .workflowName(workflowName)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }

    @Value.Immutable
    interface AttemptTarget
    {
        Optional<Integer> getId();
        Optional<String> getName();
        Optional<Integer> getSessionId();
        Optional<Integer> getProjectId();
        Optional<String> getProjectName();
    }

    static AttemptTarget buildAttemptTarget(Optional<Integer> id,
            Optional<String> name,
            Optional<Integer> sessionId,
            Optional<Integer> projectId,
            Optional<String> projectName)
    {
        return ImmutableAttemptTarget.builder()
                .id(id)
                .name(name)
                .sessionId(sessionId)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }

    // for project resource
    // for workflow resource
    // for session resource
    // for version resource
    // for log resource
    // for admin resource
    // for ui resource

    // for attempt resource
    default boolean checkGetAttempt(int siteId, Config userInfo, AttemptTarget target)
    {
        return true;
    }

    default boolean checkRunAttempt(int siteId, Config userInfo, AttemptTarget target)
    {
        return true;
    }

    default boolean checkKillAttempt(int siteId, Config userInfo, AttemptTarget target)
    {
        return true;
    }

    default boolean checkListAttemptsOfProject(int siteId, Config userInfo, ProjectTarget target)
    {
        return true;
    }

    default boolean checkListAttemptsOfWorkflow(int siteId, Config userInfo, WorkflowTarget target)
    {
        return true;
    }

    default boolean checkListAttemptsOfSession(int siteId, Config userInfo, SessionTarget target)
    {
        return true;
    }

    default List<String> getListAttemptsFilter(int siteId, Config userInfo)
    {
        return ImmutableList.<String>of();
        // return "wf.name like ''"
        // return "p.name like ''"
    }

    // for authenticated resource
    // for schedule resource
}

package io.digdag.spi;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

public interface AccessController
{
    @Value.Immutable
    interface ProjectTarget
    {
        int getId();

        String getName();

        static ProjectTarget of(int id, String name)
        {
            return ImmutableProjectTarget.builder()
                    .id(id)
                    .name(name)
                    .build();
        }
    }

    @Value.Immutable
    interface WorkflowTarget
    {
        String getName();

        int getProjectId();

        String getProjectName();

        static WorkflowTarget of(String name, int projectId, String projectName)
        {
            return ImmutableWorkflowTarget.builder()
                    .name(name)
                    .projectId(projectId)
                    .projectName(projectName)
                    .build();
        }
    }

    @Value.Immutable
    interface SessionTarget
    {
        int getId();

        String getWorkflowName();

        int getProjectId();

        String getProjectName();

        static SessionTarget of(int id, String workflowName, int projectId, String projectName)
        {
            return ImmutableSessionTarget.builder()
                    .id(id)
                    .workflowName(workflowName)
                    .projectId(projectId)
                    .projectName(projectName)
                    .build();
        }
    }

    @Value.Immutable
    interface AttemptTarget
    {
        // TODO

        Optional<Integer> getId();

        Optional<String> getName();

        Optional<Integer> getSessionId();

        Optional<Integer> getProjectId();

        Optional<String> getProjectName();

        static AttemptTarget of(Optional<Integer> id,
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
    }

    interface ListFilter
    {
        String getSql();
    }

    // for project resource
    // for workflow resource
    // for session resource
    // for version resource
    // for log resource
    // for admin resource
    // for ui resource

    // for attempt resource
    default void checkGetAttempt(int siteId, Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkRunAttempt(int siteId, Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkKillAttempt(int siteId, Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkListAttemptsOfProject(int siteId, Config userInfo, ProjectTarget target)
            throws AccessControlException
    { }

    default void checkListAttemptsOfWorkflow(int siteId, Config userInfo, WorkflowTarget target)
            throws AccessControlException
    { }

    default ListFilter getListAttemptsFilter(int siteId, Config userInfo, ProjectTarget target)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilter(int siteId, Config userInfo, WorkflowTarget target)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilter(int siteId, Config userInfo)
    {
        return () -> "true";
    }

    // for authenticated resource
    // for schedule resource
}

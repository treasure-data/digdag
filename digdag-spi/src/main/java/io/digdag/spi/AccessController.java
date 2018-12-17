package io.digdag.spi;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

@Value.Enclosing
public interface AccessController
{
    @Value.Immutable
    interface SiteTarget
    {
        int getSiteId();

        static SiteTarget of(int siteId)
        {
            return ImmutableAccessController.SiteTarget.builder()
                    .siteId(siteId)
                    .build();
        }
    }

    @Value.Immutable
    interface ProjectTarget
    {
        int getSiteId();

        int getId();

        String getName();

        // TODO better to have revision info?

        static ProjectTarget of(int siteId, int id, String name)
        {
            return ImmutableAccessController.ProjectTarget.builder()
                    .siteId(siteId)
                    .id(id)
                    .name(name)
                    .build();
        }
    }

    @Value.Immutable
    interface WorkflowTarget
    {
        int getSiteId();

        String getName();

        int getProjectId();

        String getProjectName();

        static WorkflowTarget of(int siteId, String name, int projectId, String projectName)
        {
            return ImmutableAccessController.WorkflowTarget.builder()
                    .siteId(siteId)
                    .name(name)
                    .projectId(projectId)
                    .projectName(projectName)
                    .build();
        }
    }

    @Value.Immutable
    interface SessionTarget
    {
        int getSiteId();

        int getId();

        String getWorkflowName();

        int getProjectId();

        String getProjectName();

        static SessionTarget of(int siteId, int id, String workflowName, int projectId, String projectName)
        {
            return ImmutableAccessController.SessionTarget.builder()
                    .siteId(siteId)
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

        Optional<Integer> getSiteId();

        Optional<Integer> getId();

        Optional<String> getName();

        Optional<Integer> getSessionId();

        Optional<Integer> getProjectId();

        Optional<String> getProjectName();

        static AttemptTarget of(Optional<Integer> siteId,
                Optional<Integer> id,
                Optional<String> name,
                Optional<Integer> sessionId,
                Optional<Integer> projectId,
                Optional<String> projectName)
        {
            return ImmutableAccessController.AttemptTarget.builder()
                    .siteId(siteId)
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
    default void checkGetAttempt(Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkRunAttempt(Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkKillAttempt(Config userInfo, AttemptTarget target)
            throws AccessControlException
    { }

    default void checkListAttemptsOfProject(Config userInfo, ProjectTarget target)
            throws AccessControlException
    { }

    default void checkListAttemptsOfWorkflow(Config userInfo, WorkflowTarget target)
            throws AccessControlException
    { }

    default void checkListAttempsOfSite(Config userInfo, SiteTarget target)
            throws AccessControlException
    { }

    default ListFilter getListAttemptsFilterOfProject(Config userInfo, ProjectTarget target)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilterOfWorkflow(Config userInfo, WorkflowTarget target)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilterOfSite(Config userInfo, SiteTarget target)
    {
        return () -> "true";
    }

    // for authenticated resource
    // for schedule resource
}

package io.digdag.spi.ac;

import io.digdag.client.config.Config;

public interface AccessController
{
    interface ListFilter
    {
        String getSql();
    }

    // Projects

    default void checkGetProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkPutProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkDeleteProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListProjectsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    // Workflows

    default void checkGetWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListWorkflowsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListWorkflows(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListWorkflowsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    // Sessions

    default void checkListSessionsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSessionsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkGetSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListSessionsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    /**
     * Return a filter to check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListSessionsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    // for version resource

    /** not support any filters for version resource */

    // Log

    default void checkListLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkGetLogFile(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkPutLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    // Admins

    // Uis

    // Attempts

    default void checkListAttemptsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListOtherAttemptsOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkGetAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to run the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkRunAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkKillAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Return a filter to check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListAttemptsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    // Schedules

    default void checkGetSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSchedulesOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSchedulesOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkSkipSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkBackfillSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkDisableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkEnableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListSchedulesFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }
}

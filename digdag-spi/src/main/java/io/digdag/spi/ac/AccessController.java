package io.digdag.spi.ac;

import io.digdag.client.config.Config;

public interface AccessController
{
    interface ListFilter
    {
        String getSql();
    }

    // Projects

    default void checkGetProjectOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListProjectsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    default void checkPutProjectOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    // FIXME TODO not necessary

    default void checkPutProjectOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkDeleteProjectOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    // Workflows

    default ListFilter getListWorkflowsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    default void checkGetWorkflowOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListWorkflowsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListWorkflowsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    // Sessions

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

    default ListFilter getListSessionsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    default void checkListSessionsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSessionsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkGetSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    // for version resource

    /** not support any filters for version resource */

    // Log

    default void checkListLogFilesOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkGetLogFileOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkPutLogFilesOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    // Admins

    // Uis

    // Attempts

    default void checkListAttemptsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListAttemptsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListAttemptsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    default ListFilter getListAttemptsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

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

    default void checkGetAttemptOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to run the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkRunAttemptOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkKillAttemptOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    // Schedules

    default void checkGetScheduleOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSchedulesOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    default void checkListSchedulesOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkSkipScheduleOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkBackfillScheduleOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkDisableScheduleOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default void checkEnableScheduleOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    default ListFilter getListSchedulesFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }
}

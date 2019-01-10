package io.digdag.spi.ac;

import io.digdag.client.config.Config;

public interface AccessController
{
    interface ListFilter
    {
        String getSql();
    }

    //
    // Projects
    //

    /**
     * Check if the user has permissions to get the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to put a project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkPutProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to delete the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkDeleteProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Return a filter to check if the user has permission to list projects.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListProjectsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    //
    // Workflows
    //

    /**
     * Check if the user has permissions to get the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list workflows.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListWorkflowsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list workflows.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListWorkflows(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Return a filter to check if the user has permission to list workflows.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListWorkflowsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    /**
     * Return a filter to check if the user has permission to list workflows.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    //
    // Sessions
    //

    /**
     * Check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSessionsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSessionsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to get the session.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     *
     * @param target
     * @param user
     * @return
     */
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

    /**
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    //
    // Logs
    //

    /**
     * Check if the user has permissions to list log files.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to get log file.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetLogFile(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to put log files.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkPutLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    //
    // Attempts
    //

    /**
     * Check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListAttemptsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListAttemptsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListAttemptsOfSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListAttemptsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list other attempts of the attempt.     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListOtherAttemptsOfAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to get the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
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

    /**
     * Check if the user has permissions to kill the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
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

    /**
     * Return a filter to check if the user has permission to list attempts.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListAttemptsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    /**
     * Return a filter to check if the user has permission to list attempts.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListAttemptsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    //
    // Schedules
    //

    /**
     * Check if the user has permissions to get the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSchedulesOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSchedulesOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to skip the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkSkipSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to backfill the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkBackfillSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to disable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkDisableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to enable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkEnableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Return a filter to check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListSchedulesFilterOfSite(SiteTarget target, Config user)

    {
        return () -> "true";
    }
}

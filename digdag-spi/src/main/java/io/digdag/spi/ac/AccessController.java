package io.digdag.spi.ac;

import io.digdag.client.config.Config;

public interface AccessController
{
    interface ListFilter
    {
        String getSql();
    }

    ////
    // Projects
    //

    // put, delete, get

    /**
     * Check if the user has permissions to put the project.
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
     * Check if the user has permissions to get the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    // listing

    /**
     * Check if the user has permissions to list projects.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListProjectsOfSite(SiteTarget target, Config user)
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

    // resource actions

    /**
     * Check if the user has permissions to download the project archive.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetProjectArchive(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to put a secret to the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkPutProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to delete the project secret.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkDeleteProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to get secrets from the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetProjectSecrets(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    //
    // Workflows
    //

    // put, delete, get

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

    // listing

    /**
     * Check if the user has permissions to list workflows.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListWorkflowsOfSite(SiteTarget target, Config user)
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

    // resource actions

    /**
     * Check if the user has permissions to run the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkRunWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to put log files.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkPutLogFile(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to get any log files.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    //
    // Sessions
    //

    // put, delete, get

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

    // listing

    /**
     * Check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSessionsOfSite(SiteTarget target, Config user)
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
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    // resource actions

    /**
     * Check if the user has permissions to list attempts.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetAttemptsOfSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    /**
     * Check if the user has permissions to list other attempts of the attempt.     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetOtherAttemptsOfAttempt(WorkflowTarget target, Config user)
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
     * Check if the user has permissions to get tasks of an attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetTasksOfAttempt(WorkflowTarget target, Config user)
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

    //
    // Schedules
    //

    // put, delete, get

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

    // listing

    /**
     * Check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkListSchedulesOfSite(SiteTarget target, Config user)
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
     * Return a filter to check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @return
     */
    default ListFilter getListSchedulesFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    // resource actions

    /**
     * Check if the user has permissions to list schedules.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    default void checkGetScheduleOfWorkflow(WorkflowTarget target, Config user)
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
}

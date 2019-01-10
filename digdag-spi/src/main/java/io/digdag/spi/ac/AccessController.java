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
     * Check if the user has permissions to put a project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkPutProject(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to delete the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDeleteProject(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProject(ProjectTarget target, Config user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list projects within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListProjectsOfSite(SiteTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed projects within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListProjectsFilterOfSite(SiteTarget target, Config user);

    // resource actions

    /**
     * Check if the user has permissions to download archive of the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProjectArchive(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to put a secret to the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkPutProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to delete a project from the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDeleteProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get any secrets from the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProjectSecrets(ProjectTarget target, Config user)
            throws AccessControlException;

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
    void checkGetWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list workflows within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListWorkflowsOfSite(SiteTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed workflows within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListWorkflowsFilterOfSite(SiteTarget target, Config user);

    /**
     * Check if the user has permissions to list workflows within the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListWorkflowsOfProject(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed workflows within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, Config user);

    // resource actions

    /**
     * Check if the user has permissions to run the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkRunWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to put log files to the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkPutLogFile(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get any log files from the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException;

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
    void checkGetSession(WorkflowTarget target, Config user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list sessions within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfSite(SiteTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfSite(SiteTarget target, Config user);

    /**
     * Check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfProject(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfProject(ProjectTarget target, Config user);

    /**
     * Check if the user has permissions to list sessions within the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the workflow.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, Config user);

    // resource actions

    /**
     * Check if the user has permissions to get attempts from the session.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetAttemptsFromSession(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetAttempt(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get tasks from the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetTasksFromAttempt(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to kill the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkKillAttempt(WorkflowTarget target, Config user)
            throws AccessControlException;

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
    void checkGetSchedule(WorkflowTarget target, Config user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list schedules within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSchedulesOfSite(SiteTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed schedules within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSchedulesFilterOfSite(SiteTarget target, Config user);

    /**
     * Check if the user has permissions to list schedules within the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSchedulesOfProject(ProjectTarget target, Config user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed schedules within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSchedulesFilterOfProject(ProjectTarget target, Config user);

    // resource actions

    /**
     * Check if the user has permissions to get schedules from the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetScheduleFromWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to skip the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkSkipSchedule(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to backfill the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkBackfillSchedule(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to disable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDisableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to enable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkEnableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException;
}

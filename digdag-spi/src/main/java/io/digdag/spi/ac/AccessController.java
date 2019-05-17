package io.digdag.spi.ac;

import io.digdag.spi.AuthenticatedUser;

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
    void checkPutProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to delete the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDeleteProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list projects within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListProjectsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed projects within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListProjectsFilterOfSite(SiteTarget target, AuthenticatedUser user);

    // resource actions

    /**
     * Check if the user has permissions to download archive of the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProjectArchive(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to put a secret to the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkPutProjectSecret(SecretTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to delete a project from the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDeleteProjectSecret(SecretTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get any secrets from the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetProjectSecretList(ProjectTarget target, AuthenticatedUser user)
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
    void checkGetWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list workflows within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListWorkflowsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed workflows within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListWorkflowsFilterOfSite(SiteTarget target, AuthenticatedUser user);

    /**
     * Check if the user has permissions to list workflows within the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListWorkflowsOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed workflows within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, AuthenticatedUser user);

    // resource actions

    /**
     * Check if the user has permissions to run the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkRunWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to put log files to the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkPutLogFile(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get any log files from the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetLogFiles(WorkflowTarget target, AuthenticatedUser user)
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
    void checkGetSession(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list sessions within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfSite(SiteTarget target, AuthenticatedUser user);

    /**
     * Check if the user has permissions to list sessions.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfProject(ProjectTarget target, AuthenticatedUser user);

    /**
     * Check if the user has permissions to list sessions within the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSessionsOfWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed sessions within the workflow.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, AuthenticatedUser user);

    // resource actions

    /**
     * Check if the user has permissions to get attempts from the session.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetAttemptsFromSession(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetAttempt(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to get tasks from the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetTasksFromAttempt(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to kill the attempt.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkKillAttempt(AttemptTarget target, AuthenticatedUser user)
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
    void checkGetSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    // listing

    /**
     * Check if the user has permissions to list schedules within the site.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSchedulesOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed schedules within the site.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSchedulesFilterOfSite(SiteTarget target, AuthenticatedUser user);

    /**
     * Check if the user has permissions to list schedules within the project.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkListSchedulesOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Return a filter to return only allowed schedules within the project.
     *
     * @param target
     * @param user
     * @return
     */
    ListFilter getListSchedulesFilterOfProject(ProjectTarget target, AuthenticatedUser user);

    // resource actions

    /**
     * Check if the user has permissions to get schedules from the workflow.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkGetScheduleFromWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to skip the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkSkipSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to backfill the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkBackfillSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to disable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkDisableSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;

    /**
     * Check if the user has permissions to enable the schedule.
     *
     * @param target
     * @param user
     * @throws AccessControlException
     */
    void checkEnableSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException;
}

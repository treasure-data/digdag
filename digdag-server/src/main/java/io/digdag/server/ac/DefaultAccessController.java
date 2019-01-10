package io.digdag.server.ac;

import io.digdag.client.config.Config;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;

public class DefaultAccessController
        implements AccessController
{
    @Override
    public void checkPutProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkDeleteProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkListProjectsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListProjectsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetProjectArchive(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkPutProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkDeleteProjectSecret(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetProjectSecrets(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkListWorkflowsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListWorkflowsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkListWorkflowsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkRunWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkPutLogFile(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetLogFiles(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkListSessionsOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSessionsOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSessionsOfWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetAttemptsFromSession(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetTasksFromAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkKillAttempt(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkGetSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkListSchedulesOfSite(SiteTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSchedulesFilterOfSite(SiteTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSchedulesOfProject(ProjectTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSchedulesFilterOfProject(ProjectTarget target, Config user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetScheduleFromWorkflow(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkSkipSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkBackfillSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkDisableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }

    @Override
    public void checkEnableSchedule(WorkflowTarget target, Config user)
            throws AccessControlException
    { }
}

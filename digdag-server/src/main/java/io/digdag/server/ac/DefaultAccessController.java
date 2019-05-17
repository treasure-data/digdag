package io.digdag.server.ac;

import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SecretTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;

public class DefaultAccessController
        implements AccessController
{
    @Override
    public void checkPutProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkDeleteProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkListProjectsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListProjectsFilterOfSite(SiteTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetProjectArchive(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkPutProjectSecret(SecretTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkDeleteProjectSecret(SecretTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetProjectSecretList(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkListWorkflowsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListWorkflowsFilterOfSite(SiteTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkListWorkflowsOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListWorkflowsFilterOfProject(ProjectTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkRunWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkPutLogFile(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetLogFiles(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetSession(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkListSessionsOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfSite(SiteTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSessionsOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfProject(ProjectTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSessionsOfWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSessionsFilterOfWorkflow(WorkflowTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetAttemptsFromSession(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetAttempt(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetTasksFromAttempt(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkKillAttempt(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkGetSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkListSchedulesOfSite(SiteTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSchedulesFilterOfSite(SiteTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkListSchedulesOfProject(ProjectTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public ListFilter getListSchedulesFilterOfProject(ProjectTarget target, AuthenticatedUser user)
    {
        return () -> "true";
    }

    @Override
    public void checkGetScheduleFromWorkflow(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkSkipSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkBackfillSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkDisableSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }

    @Override
    public void checkEnableSchedule(WorkflowTarget target, AuthenticatedUser user)
            throws AccessControlException
    { }
}

package io.digdag.spi.ac;

import io.digdag.client.config.Config;

public interface AccessController
{

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

package io.digdag.server.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkflowService
{
    private static Logger logger = LoggerFactory.getLogger(WorkflowService.class);
    private final TransactionManager tm;
    private final ProjectStoreManager rm;
    private final AccessController ac;

    @Inject
    public WorkflowService(final ProjectStoreManager rm, final AccessController ac, final TransactionManager tm)
    {
        this.tm = tm;
        this.rm = rm;
        this.ac = ac;
    }

    public <T>T getWorkflows(int siteId, AuthenticatedUser authenticatedUser, Optional<Long> lastId, Optional<Integer> count, String orderDirection,
                             Optional<String> namePattern, boolean searchProjectName,
                             Function<List<StoredWorkflowDefinitionWithProject>, T> func)
            throws ResourceNotFoundException, AccessControlException
    {
        final SiteTarget siteTarget = SiteTarget.of(siteId);
        ac.checkListWorkflowsOfSite(siteTarget, authenticatedUser);  // AccessControl
        return tm.<T, ResourceNotFoundException, AccessControlException>begin(() -> {
            List<StoredWorkflowDefinitionWithProject> defs =
                    rm.getProjectStore(siteId)
                            .getLatestActiveWorkflowDefinitions(
                                    count.or(100),
                                    lastId, // check NotFound first
                                    orderAscending(orderDirection),
                                    namePattern,
                                    searchProjectName,
                                    ac.getListWorkflowsFilterOfSite(SiteTarget.of(siteId), authenticatedUser));

            return func.apply(defs);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    public <T>T getWorkflow(int siteId, AuthenticatedUser authenticatedUser, long id, Function<StoredWorkflowDefinitionWithProject, T> func)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<T, ResourceNotFoundException, AccessControlException>begin(() -> {
            StoredWorkflowDefinitionWithProject def =
                    rm.getProjectStore(siteId)
                            .getWorkflowDefinitionById(id); // check NotFound first

            ac.checkGetWorkflow( // AccessControl
                    WorkflowTarget.of(siteId, def.getName(), def.getProject().getName()), authenticatedUser);

            return func.apply(def);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    private boolean orderAscending(String orderDirection)
    {
        if (orderDirection == null || orderDirection.equals("asc")) {
            return true;
        }
        else if (orderDirection.equals("desc")) {
            return false;
        }
        else {
            throw new IllegalArgumentException("parameter 'order' must be either 'asc' or 'desc'");
        }
    }
}

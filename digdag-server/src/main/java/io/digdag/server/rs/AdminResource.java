package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.SessionTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.swagger.annotations.Api;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Api("Admin")
@AdminRestricted
@Path("/")
@Produces("application/json")
public class AdminResource
        extends AuthenticatedResource
{
    private final ProjectStoreManager pm;
    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final AccessController ac;

    @Inject
    public AdminResource(
            ProjectStoreManager pm,
            SessionStoreManager sm,
            TransactionManager tm,
            AccessController ac)
    {
        this.pm = pm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
    }

    @GET
    @Path("/api/admin/attempts/{id}/userinfo")
    public Config getUserInfo(@PathParam("id") long id)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            StoredSessionAttemptWithSession session = sm.getAttemptWithSessionById(id, // NotFound
                    () -> ac.getGetUserInfoFilterOf(
                            SiteTarget.of(getSiteId()),
                            getUserInfo())
            );


            if (!session.getWorkflowDefinitionId().isPresent()) {
                // TODO: is 404 appropriate in this situation?
                throw new NotFoundException();
            }

            final ProjectStore rs = pm.getProjectStore(getSiteId());
            final StoredProject proj = rs.getProjectById(session.getSession().getProjectId(), // NotFound
                    () -> ac.getGetUserInfoFilterOf( // TODO need to revisit to decide that we can use getGetProjectFilter method
                            SiteTarget.of(getSiteId()),
                            getUserInfo())
            );

            StoredRevision revision = pm.getRevisionOfWorkflowDefinition(session.getWorkflowDefinitionId().get(), // NotFound
                    () -> {
                        final WorkflowTarget wfTarget = WorkflowTarget.of(getSiteId(),
                                session.getSession().getWorkflowName(),
                                Optional.of(proj.getId()),
                                proj.getName());
                        return ac.getGetUserInfoFilterOf(wfTarget, getUserInfo()); // TODO need to revisit to decide that we can use getGetProjectFilter method
                    }
            );


            return revision.getUserInfo();
        }, ResourceNotFoundException.class);
    }
}

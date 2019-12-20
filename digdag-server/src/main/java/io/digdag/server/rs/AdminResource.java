package io.digdag.server.rs;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
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

    @Inject
    public AdminResource(
            ProjectStoreManager pm,
            SessionStoreManager sm,
            TransactionManager tm)
    {
        this.pm = pm;
        this.sm = sm;
        this.tm = tm;
    }

    @Deprecated
    @GET
    @Path("/api/admin/attempts/{id}/userinfo")
    public Config getUserInfo(@PathParam("id") long id)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            StoredSessionAttemptWithSession session = sm.getAttemptWithSessionById(id);

            if (!session.getWorkflowDefinitionId().isPresent()) {
                // TODO: is 404 appropriate in this situation?
                throw new NotFoundException();
            }

            StoredRevision revision = pm.getRevisionOfWorkflowDefinition(session.getWorkflowDefinitionId().get());

            return revision.getUserInfo();
        }, ResourceNotFoundException.class);
    }
}

package io.digdag.server;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import io.digdag.core.config.*;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;

@Path("/")
@Produces("application/json")
public class WorkflowResource
{
    // [*] GET  /api/workflows                                   # list workflows of the latest revision of all repositories
    // [*] GET  /api/workflows/<id>                              # show a particular workflow (which belongs to a revision)

    private final RepositoryStoreManager rm;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public WorkflowResource(
            RepositoryStoreManager rm)
    {
        this.rm = rm;
    }

    @GET
    @Path("/api/workflows")
    public List<RestWorkflow> getWorkflows()
    {
        // TODO paging
        final ImmutableList.Builder<RestWorkflow> builder = ImmutableList.builder();
        return rm.getRepositoryStore(siteId).getLatestActiveWorkflows(100, Optional.absent())
            .stream()
            .map(wfDetails -> RestWorkflow.of(wfDetails))
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/workflows/{id}")
    public RestWorkflow getWorkflow(@PathParam("id") int wfId)
            throws ResourceNotFoundException
    {
        StoredWorkflowSource wf = rm.getRepositoryStore(siteId).getWorkflowById(wfId);
        StoredRevision rev = rm.getRepositoryStore(siteId).getRevisionById(wf.getRevisionId());
        StoredRepository repo = rm.getRepositoryStore(siteId).getRepositoryById(rev.getRepositoryId());
        return RestWorkflow.of(repo, rev, wf);
    }
}

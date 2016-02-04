package io.digdag.server;

import java.time.Instant;
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
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.client.api.*;

@Path("/")
@Produces("application/json")
public class WorkflowResource
    extends AuthenticatedResource
{
    // [*] GET  /api/workflows                                   # list workflows of the latest revision of all repositories

    private final RepositoryStoreManager rm;

    @Inject
    public WorkflowResource(
            RepositoryStoreManager rm)
    {
        this.rm = rm;
    }

    //@GET
    //@Path("/api/workflows")
    //public List<RestWorkflowDefinition> getWorkflows()
    //{
    //    // TODO paging
    //    final ImmutableList.Builder<RestWorkflowDefinition> builder = ImmutableList.builder();
    //    return rm.getRepositoryStore(getSiteId()).getLatestWorkflowDefinitions(100, Optional.absent())
    //        .stream()
    //        .map(wfDetails -> RestModels.workflowDefinition(wfDetails))
    //        .collect(Collectors.toList());
    //}
}

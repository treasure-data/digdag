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

    private final ConfigFactory cf;
    private final YamlConfigLoader loader;
    private final WorkflowCompiler compiler;
    private final RepositoryStoreManager rm;
    private final ScheduleStoreManager scheduleStore;
    private final SchedulerManager scheds;
    private final TempFileManager temp;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public WorkflowResource(
            ConfigFactory cf,
            YamlConfigLoader loader,
            WorkflowCompiler compiler,
            RepositoryStoreManager rm,
            ScheduleStoreManager scheduleStore,
            SchedulerManager scheds,
            TempFileManager temp)
    {
        this.cf = cf;
        this.loader = loader;
        this.scheduleStore = scheduleStore;
        this.scheds = scheds;
        this.compiler = compiler;
        this.rm = rm;
        this.temp = temp;
    }

    @GET
    @Path("/api/workflows")
    public List<RestWorkflow> getWorkflows()
    {
        // TODO paging
        // TODO n-m db access
        final ImmutableList.Builder<RestWorkflow> builder = ImmutableList.builder();
        rm.getRepositoryStore(siteId).getRepositories(100, Optional.absent())
            .stream()
            .forEach(repo -> {
                StoredRevision rev = rm.getRepositoryStore(siteId).getLatestActiveRevision(repo.getId());
                List<StoredWorkflowSource> workflows = rm.getRepositoryStore(siteId).getWorkflows(rev.getId(), 100, Optional.absent());
                for (StoredWorkflowSource wf : workflows) {
                    builder.add(RestWorkflow.of(repo, rev, wf));
                }
            });
        return builder.build();
    }

    @GET
    @Path("/api/workflows/{id}")
    public RestWorkflow getWorkflow(@PathParam("id") int wfId)
    {
        StoredWorkflowSource wf = rm.getRepositoryStore(siteId).getWorkflowById(wfId);
        StoredRevision rev = rm.getRepositoryStore(siteId).getRevisionById(wf.getRevisionId());
        StoredRepository repo = rm.getRepositoryStore(siteId).getRepositoryById(rev.getRepositoryId());
        return RestWorkflow.of(repo, rev, wf);
    }
}

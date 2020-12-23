package io.digdag.server.rs;

import java.util.List;
import java.io.IOException;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.*;
import io.digdag.core.repository.*;
import io.digdag.core.log.LogServerManager;
import io.digdag.client.api.*;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.*;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.WorkflowTarget;
import io.digdag.spi.metrics.DigdagMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import static io.digdag.core.log.LogServerManager.logFilePrefixFromSessionAttempt;

@Api("Log")
@Path("/")
@Produces("application/json")
public class LogResource
    extends AuthenticatedResource
{
    // GET  /api/logs/{attempt_id}/files[?task=<name>]
    // GET  /api/logs/{attempt_id}/files/{file_name}

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final AccessController ac;
    private final LogServer logServer;
    private final DigdagMetrics metrics;


    @Inject
    public LogResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            AccessController ac,
            LogServerManager lm,
            DigdagMetrics metrics)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
        this.logServer = lm.getLogServer();
        this.metrics = metrics;
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/logs/{attempt_id}/files")
    @ApiOperation("List log files of an attempt with filters")
    public RestLogFileHandleCollection getFileHandles(
            @ApiParam(value="attempt id", required=true)
            @PathParam("attempt_id") long attemptId,
            @ApiParam(value="partial prefix match filter on task name", required=false)
            @QueryParam("task") String taskName,
            @ApiParam(value="enable returning direct download handle", required=false)
            @QueryParam("direct_download") Boolean directDownloadAllowed)
            throws ResourceNotFoundException, AccessControlException
    {
        // Disable direct download (including direct= field in the response body where
        // some clients including digdag-client package use the link automatically when
        // it's set) if ?direct_download=false is given.
        boolean enableDirectDownload = (directDownloadAllowed == null) || (boolean) directDownloadAllowed;
        return tm.<RestLogFileHandleCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkGetLogFiles(
                            WorkflowTarget.of(getSiteId(), a.getSession().getWorkflowName(), p.getName()),
                            getAuthenticatedUser()));
            List<LogFileHandle> handles = logServer.getFileHandles(prefix, Optional.fromNullable(taskName), enableDirectDownload);
            return RestModels.logFileHandleCollection(handles);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Produces("application/gzip")
    @Path("/api/logs/{attempt_id}/files/{file_name}")
    @ApiOperation("Download a log file")
    public byte[] getFile(
            @ApiParam(value="attempt id", required=true)
            @PathParam("attempt_id") long attemptId,
            @ApiParam(value="log file name", required=true)
            @PathParam("file_name") String fileName)
            throws ResourceNotFoundException, IOException, StorageFileNotFoundException, AccessControlException
    {
        return tm.<byte[], ResourceNotFoundException, IOException, StorageFileNotFoundException, AccessControlException>begin(() -> {
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkGetLogFiles(
                            WorkflowTarget.of(getSiteId(), a.getSession().getWorkflowName(), p.getName()),
                            getAuthenticatedUser()));
            return logServer.getFile(prefix, fileName);
        }, ResourceNotFoundException.class, IOException.class, StorageFileNotFoundException.class, AccessControlException.class);
    }

    private LogFilePrefix getPrefix(final long attemptId, final AccessControlAction acAction)
            throws ResourceNotFoundException, AccessControlException
    {
        final StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                .getAttemptById(attemptId); // check NotFound first
        final StoredProject project = rm.getProjectStore(getSiteId())
                .getProjectById(attempt.getSession().getProjectId()); // check NotFound first
        acAction.call(project, attempt); // AccessControl
        return logFilePrefixFromSessionAttempt(attempt);
    }

    private interface AccessControlAction
    {
        void call(StoredProject project, StoredSessionAttemptWithSession attempt)
                throws AccessControlException;
    }
}

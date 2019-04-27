package io.digdag.server.rs;

import java.util.List;
import java.time.Instant;
import java.io.InputStream;
import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import javax.ws.rs.ServerErrorException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.*;
import io.digdag.core.repository.*;
import io.digdag.core.log.LogServerManager;
import io.digdag.client.api.*;
import io.digdag.spi.*;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.WorkflowTarget;
import io.swagger.annotations.Api;

import static io.digdag.core.log.LogServerManager.logFilePrefixFromSessionAttempt;

@Api("Log")
@Path("/")
@Produces("application/json")
public class LogResource
    extends AuthenticatedResource
{
    // PUT  /api/logs/{attempt_id}/files?task=<name>&file_time=<unixtime sec>&node_id=<node_id>
    // GET  /api/logs/{attempt_id}/files[?task=<name>]
    // GET  /api/logs/{attempt_id}/files/{file_name}
    // GET  /api/logs/{attempt_id}/upload_handle?task=<name>&file_time=<unixtime sec>&node_id=<nodeId>

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final AccessController ac;
    private final LogServer logServer;

    @Inject
    public LogResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            AccessController ac,
            LogServerManager lm)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
        this.logServer = lm.getLogServer();
    }

    @PUT
    @Consumes("application/gzip")
    @Path("/api/logs/{attempt_id}/files")
    public RestLogFilePutResult putFile(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName,
            @QueryParam("file_time") long unixFileTime,
            @QueryParam("node_id") String nodeId,
            InputStream body)
            throws ResourceNotFoundException, IOException, AccessControlException
    {
        return tm.<RestLogFilePutResult, ResourceNotFoundException, IOException, AccessControlException>begin(() -> {
            // TODO null check taskName
            // TODO null check nodeId
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkPutLogFile(
                            WorkflowTarget.of(getSiteId(), p.getName(), a.getSession().getWorkflowName()),
                            getAuthenticatedUser()));

            byte[] data = ByteStreams.toByteArray(body);
            String fileName = logServer.putFile(prefix, taskName, Instant.ofEpochSecond(unixFileTime), nodeId, data);
            return RestLogFilePutResult.of(fileName);
        }, ResourceNotFoundException.class, IOException.class, AccessControlException.class);
    }

    @GET
    @Path("/api/logs/{attempt_id}/upload_handle")
    public DirectUploadHandle getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName,
            @QueryParam("file_time") long unixFileTime,
            @QueryParam("node_id") String nodeId)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<DirectUploadHandle, ResourceNotFoundException, AccessControlException>begin(() -> {
            // TODO null check taskName
            // TODO null check nodeId
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkPutLogFile(
                            WorkflowTarget.of(getSiteId(), p.getName(), a.getSession().getWorkflowName()),
                            getAuthenticatedUser()));

            Optional<DirectUploadHandle> handle = logServer.getDirectUploadHandle(prefix, taskName, Instant.ofEpochSecond(unixFileTime), nodeId);

            if (handle.isPresent()) {
                return handle.get();
            }
            else {
                throw new ServerErrorException(
                        Response.status(Response.Status.NOT_IMPLEMENTED)
                                .type("application/json")
                                .entity("{\"message\":\"Direct upload handle is not available for this log server implementation\",\"status\":501}")
                                .build());
            }
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @GET
    @Path("/api/logs/{attempt_id}/files")
    public RestLogFileHandleCollection getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestLogFileHandleCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkGetLogFiles(
                            WorkflowTarget.of(getSiteId(), p.getName(), a.getSession().getWorkflowName()),
                            getAuthenticatedUser()));
            List<LogFileHandle> handles = logServer.getFileHandles(prefix, Optional.fromNullable(taskName));
            return RestModels.logFileHandleCollection(handles);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @GET
    @Produces("application/gzip")
    @Path("/api/logs/{attempt_id}/files/{file_name}")
    public byte[] getFile(
            @PathParam("attempt_id") long attemptId,
            @PathParam("file_name") String fileName)
            throws ResourceNotFoundException, IOException, StorageFileNotFoundException, AccessControlException
    {
        return tm.<byte[], ResourceNotFoundException, IOException, StorageFileNotFoundException, AccessControlException>begin(() -> {
            final LogFilePrefix prefix = getPrefix(attemptId, // NotFound, AccessControl
                    (p, a) -> ac.checkGetLogFiles(
                            WorkflowTarget.of(getSiteId(), p.getName(), a.getSession().getWorkflowName()),
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

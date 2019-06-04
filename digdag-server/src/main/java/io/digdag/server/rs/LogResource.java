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
import io.digdag.metrics.DigdagMetrics;
import io.digdag.spi.*;
import io.micrometer.core.annotation.Timed;
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

    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final LogServer logServer;
    private final DigdagMetrics metrics;


    @Inject
    public LogResource(
            SessionStoreManager sm,
            TransactionManager tm,
            LogServerManager lm,
            DigdagMetrics metrics)
    {
        this.sm = sm;
        this.tm = tm;
        this.logServer = lm.getLogServer();
        this.metrics = metrics;
    }


    @Timed(value="API_PutLogFile")
    @PUT
    @Consumes("application/gzip")
    @Path("/api/logs/{attempt_id}/files")
    public RestLogFilePutResult putFile(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName,
            @QueryParam("file_time") long unixFileTime,
            @QueryParam("node_id") String nodeId,
            InputStream body)
            throws ResourceNotFoundException, IOException
    {
        return tm.<RestLogFilePutResult, ResourceNotFoundException, IOException>begin(() -> {
            // TODO null check taskName
            // TODO null check nodeId
            LogFilePrefix prefix = getPrefix(attemptId);

            byte[] data = ByteStreams.toByteArray(body);
            String fileName = logServer.putFile(prefix, taskName, Instant.ofEpochSecond(unixFileTime), nodeId, data);
            return RestLogFilePutResult.of(fileName);
        }, ResourceNotFoundException.class, IOException.class);
    }

    @Timed(value="API_GetLogUploadHandle")
    @GET
    @Path("/api/logs/{attempt_id}/upload_handle")
    public DirectUploadHandle getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName,
            @QueryParam("file_time") long unixFileTime,
            @QueryParam("node_id") String nodeId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            // TODO null check taskName
            // TODO null check nodeId
            LogFilePrefix prefix = getPrefix(attemptId);

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
        }, ResourceNotFoundException.class);
    }

    @Timed(value="API_GetLogFileHandles")
    @GET
    @Path("/api/logs/{attempt_id}/files")
    public RestLogFileHandleCollection getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            LogFilePrefix prefix = getPrefix(attemptId);
            List<LogFileHandle> handles = logServer.getFileHandles(prefix, Optional.fromNullable(taskName));
            return RestModels.logFileHandleCollection(handles);
        }, ResourceNotFoundException.class);
    }

    @Timed(value="API_GetLogFile")
    @GET
    @Produces("application/gzip")
    @Path("/api/logs/{attempt_id}/files/{file_name}")
    public byte[] getFile(
            @PathParam("attempt_id") long attemptId,
            @PathParam("file_name") String fileName)
            throws ResourceNotFoundException, IOException, StorageFileNotFoundException
    {
        return tm.<byte[], ResourceNotFoundException, IOException, StorageFileNotFoundException>begin(() -> {
            LogFilePrefix prefix = getPrefix(attemptId);
            return logServer.getFile(prefix, fileName);
        }, ResourceNotFoundException.class, IOException.class, StorageFileNotFoundException.class);
    }

    private LogFilePrefix getPrefix(long attemptId)
        throws ResourceNotFoundException
    {
        StoredSessionAttemptWithSession attempt =
                sm.getSessionStore(getSiteId())
                        .getAttemptById(attemptId);
        return logFilePrefixFromSessionAttempt(attempt);
    }
}

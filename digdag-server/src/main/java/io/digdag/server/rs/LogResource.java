package io.digdag.server.rs;

import java.util.List;
import java.time.Instant;
import java.util.stream.Collectors;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
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
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.*;
import io.digdag.core.repository.*;
import io.digdag.core.log.LogServerManager;
import io.digdag.client.api.*;
import io.digdag.spi.*;

import static io.digdag.core.log.LogServerManager.logFilePrefixFromSessionAttempt;

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
    private final LogServer logServer;

    @Inject
    public LogResource(
            SessionStoreManager sm,
            LogServerManager lm)
    {
        this.sm = sm;
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
        throws ResourceNotFoundException, IOException
    {
        // TODO null check taskName
        // TODO null check nodeId
        LogFilePrefix prefix = getPrefix(attemptId);

        byte[] data = ByteStreams.toByteArray(body);
        String fileName = logServer.putFile(prefix, taskName, Instant.ofEpochSecond(unixFileTime), nodeId, data);
        return RestLogFilePutResult.of(fileName);
    }

    @GET
    @Path("/api/logs/{attempt_id}/upload_handle")
    public DirectUploadHandle getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName,
            @QueryParam("file_time") long unixFileTime,
            @QueryParam("node_id") String nodeId)
        throws ResourceNotFoundException
    {
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
    }

    @GET
    @Path("/api/logs/{attempt_id}/files")
    public RestLogFileHandleCollection getFileHandles(
            @PathParam("attempt_id") long attemptId,
            @QueryParam("task") String taskName)
        throws ResourceNotFoundException
    {
        LogFilePrefix prefix = getPrefix(attemptId);
        List<LogFileHandle> handles = logServer.getFileHandles(prefix, Optional.fromNullable(taskName));

        return RestModels.logFileHandleCollection(handles);
    }

    @GET
    @Produces("application/gzip")
    @Path("/api/logs/{attempt_id}/files/{file_name}")
    public byte[] getFile(
            @PathParam("attempt_id") long attemptId,
            @PathParam("file_name") String fileName)
        throws StorageFileNotFoundException, ResourceNotFoundException
    {
        LogFilePrefix prefix = getPrefix(attemptId);
        return logServer.getFile(prefix, fileName);
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

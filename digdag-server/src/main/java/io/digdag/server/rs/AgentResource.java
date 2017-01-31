package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.api.RestAgentLauncherHandle;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.server.AgentLauncherConfig;
import io.digdag.server.GenericJsonExceptionHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Produces("application/json")
public class AgentResource
{
    // GET  /api/agent/launcher_handle                       # get agent launcher handle
    // GET  /api/agent/jar                                   # download agent jar file

    private static final Logger logger = LoggerFactory.getLogger(AgentResource.class);

    private final Provider<AgentLauncherConfig> launcherConfigProvider;

    @Inject
    public AgentResource(
            Provider<AgentLauncherConfig> launcherConfigProvider)
    {
        this.launcherConfigProvider = launcherConfigProvider;
    }

    @GET
    @Path("/api/agent/launcher_handle")
    public RestAgentLauncherHandle getLauncherHandle()
    {
        AgentLauncherConfig config = launcherConfigProvider.get();

        Optional<RestDirectDownloadHandle> direct;
        if (config.getDownloadUrl().isPresent()) {
            direct = Optional.of(RestDirectDownloadHandle.of(config.getDownloadUrl().get()));
        }
        else if (config.getLocalJarPath().isPresent()) {
            direct = Optional.absent();
        }
        else {
            throw new NotFoundException(
                    GenericJsonExceptionHandler
                    .toResponse(Response.Status.NOT_FOUND, "Remote agent launcher is not available"));
        }

        return RestAgentLauncherHandle.builder()
            .direct(direct)
            .md5(config.getMd5().get())
            .javaOptions(config.getJavaOptions())
            .javaArguments(config.getJavaArguments())
            .build();
    }

    @GET
    @Path("/api/agent/jar")
    @Produces("application/java-archive")
    public Response getAgentJar()
    {
        AgentLauncherConfig config = launcherConfigProvider.get();

        if (config.getDownloadUrl().isPresent()) {
            // redirect to a remote download url
            try {
                return Response.seeOther(URI.create(config.getDownloadUrl().get())).build();
            }
            catch (IllegalArgumentException ex) {
                logger.error("Failed to create a HTTP response to redirect /api/agent/jar to a direct download URL.", ex);
                throw new InternalServerErrorException(
                    GenericJsonExceptionHandler
                    .toResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed to create a HTTP response to redirect /api/agent/jar to a direct download URL: " + ex));
            }
        }
        else if (config.getLocalJarPath().isPresent()) {
            java.nio.file.Path localPath = Paths.get(config.getLocalJarPath().get());

            return Response.ok(new StreamingOutput() {
                    @Override
                    public void write(OutputStream out)
                        throws IOException, WebApplicationException
                    {
                        try (InputStream in = Files.newInputStream(localPath)) {
                            ByteStreams.copy(in, out);
                        }
                    }
                })
                .header("Content-Md5", config.getMd5().get())
                .build();
        }
        else {
            throw new NotFoundException(
                    GenericJsonExceptionHandler
                    .toResponse(Response.Status.NOT_FOUND, "Remote agent launcher is not available"));
        }
    }
}

package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.digdag.server.ServerConfig;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import javax.annotation.Resource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Serves a React.JS application.
 *
 * See :digdag-ui:buildUi task defined in build.gradle for the assets copied
 * in /public/*.
 */
@Path("/")
public class UiResource
{
    private final ServerConfig serverConfig;
    private final ClassLoader classLoader;
    private final Response indexResponse;

    @Inject
    public UiResource(final ServerConfig serverConfig)
        throws IOException
    {
        this.serverConfig = serverConfig;
        this.classLoader = UiResource.class.getClassLoader();
        this.indexResponse = getResourceIfExists("public/index.html")
            .or(Response.status(NOT_FOUND).build());
    }

    @GET
    @Path("/assets/{path}")
    public Response getAssets(@PathParam("path") String path)
        throws IOException
    {
        return getResourceIfExists("public/assets/" + path)
            .or(() -> Response.status(NOT_FOUND).build());
    }

    @GET
    @Path("/{path:.*}")
    public Response getApplication(@PathParam("path") String path)
        throws IOException
    {
        if (path.isEmpty()) {
            return indexResponse;
        }
        return getResourceIfExists("public/" + path)
            .or(indexResponse);
    }

    @GET
    @Path("/config.js")
    public Response getConfigJs()
            throws IOException
    {
        final Optional<String> uiConfigJsPath = serverConfig.getUiConfigJsPath();
        if (uiConfigJsPath.isPresent()) {
            final Optional<Response> response = getResourceIfExists(uiConfigJsPath.get());
            if (response.isPresent()) {
                return response.get();
            }
        }
        return getApplication("config.js");
    }

    private Optional<Response> getResourceIfExists(String name)
        throws IOException
    {
        try {
            URL resource = Resources.getResource(name);
            byte[] bytes;
            try {
                bytes = Resources.toByteArray(resource);
            }
            catch (NullPointerException e) {
                // resource is a directory
                return Optional.absent();
            }
            Response response = Response.ok(bytes).type(URLConnection.guessContentTypeFromName(name)).build();
            return Optional.of(response);
        }
        catch (IllegalArgumentException e) {
            return Optional.absent();
        }
    }
}

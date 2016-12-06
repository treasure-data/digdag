package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
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
    private final ClassLoader classLoader;
    private final Response indexResponse;

    public UiResource()
        throws IOException
    {
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

package io.digdag.server.rs;

import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableMap;
import io.digdag.core.Version;

@Path("/")
@Produces("application/json")
public class VersionResource
{
    @Inject
    public VersionResource()
    { }

    @GET
    @Path("/api/version")
    public Map<String, Object> getVersion()
    {
        return ImmutableMap.of("version", Version.version());
    }
}

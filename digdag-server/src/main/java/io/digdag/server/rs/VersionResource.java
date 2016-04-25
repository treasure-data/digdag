package io.digdag.server.rs;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.digdag.core.Version;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.util.Map;

@Path("/")
@Produces("application/json")
public class VersionResource
{
    private final Version version;

    @Inject
    public VersionResource(Version version)
    {
        this.version = version;
    }

    @GET
    @Path("/api/version")
    public Map<String, Object> getVersion()
    {
        return ImmutableMap.of("version", version.version());
    }
}

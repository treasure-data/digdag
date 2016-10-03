package io.digdag.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;

import io.digdag.guice.rs.server.PostStart;
import io.digdag.guice.rs.server.undertow.UndertowServerInfo;
import io.digdag.server.ServerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerRuntimeInfoWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ServerRuntimeInfoWriter.class);

    private final Optional<String> serverInfoPath;
    private final UndertowServerInfo serverInfo;

    @Inject
    public ServerRuntimeInfoWriter(ServerConfig serverConfig, UndertowServerInfo serverInfo)
    {
        this.serverInfoPath = serverConfig.getServerRuntimeInfoPath();
        this.serverInfo = serverInfo;
    }

    @PostStart
    public void postStart()
    {
        if (serverInfoPath.isPresent()) {
            ServerRuntimeInfo serverRuntimeInfo = ServerRuntimeInfo.builder()
                    .addAllLocalAddresses(serverInfo.getLocalAddresses().stream()
                            .map(a -> ServerRuntimeInfo.Address.of(a.getHostString(), a.getPort()))
                            .collect(Collectors.toList()))
                    .build();
            ObjectMapper mapper = new ObjectMapper();
            try {
                Files.write(Paths.get(serverInfoPath.get()), mapper.writeValueAsBytes(serverRuntimeInfo));
            }
            catch (IOException e) {
                logger.warn("Failed to write server runtime info", e);
            }
        }
    }
}

package io.digdag.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.server.PostStart;
import io.digdag.server.ServerConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerRuntimeInfoWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ServerRuntimeInfoWriter.class);

    private final Optional<String> serverInfoPath;
    private final GuiceRsServerControl serverControl;

    @Inject
    public ServerRuntimeInfoWriter(ServerConfig serverConfig, GuiceRsServerControl serverControl)
    {
        this.serverInfoPath = serverConfig.getServerRuntimeInfoPath();
        this.serverControl = serverControl;
    }

    @PostStart
    public void postStart()
    {
        if (serverInfoPath.isPresent()) {
            List<InetSocketAddress> apiAddresses = serverControl.getListenAddresses().get(ServerConfig.API_ADDRESS);
            List<InetSocketAddress> adminAddresses = serverControl.getListenAddresses().get(ServerConfig.ADMIN_ADDRESS);

            if (apiAddresses == null) {
                apiAddresses = ImmutableList.of();
            }
            if (adminAddresses == null) {
                adminAddresses = ImmutableList.of();
            }

            ServerRuntimeInfo serverRuntimeInfo = ServerRuntimeInfo.builder()
                .localAddresses(
                        apiAddresses.stream()
                        .map(a -> ServerRuntimeInfo.Address.of(a.getHostString(), a.getPort()))
                        .collect(Collectors.toList()))
                .localAdminAddresses(
                        adminAddresses.stream()
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

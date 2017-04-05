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
            List<ServerRuntimeInfo.Address> apiAddresses = serverControl.getRuntimeInfo().getListenAddresses()
                .stream()
                .filter(a -> a.getName().equals(ServerConfig.API_ADDRESS))
                .map(a -> a.getSocketAddress())
                .map(sa -> ServerRuntimeInfo.Address.of(sa.getHostString(), sa.getPort()))
                .collect(Collectors.toList());
            List<ServerRuntimeInfo.Address> adminAddresses = serverControl.getRuntimeInfo().getListenAddresses()
                .stream()
                .filter(a -> a.getName().equals(ServerConfig.ADMIN_ADDRESS))
                .map(a -> a.getSocketAddress())
                .map(sa -> ServerRuntimeInfo.Address.of(sa.getHostString(), sa.getPort()))
                .collect(Collectors.toList());

            ServerRuntimeInfo serverRuntimeInfo = ServerRuntimeInfo.builder()
                .localAddresses(apiAddresses)
                .localAdminAddresses(adminAddresses)
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

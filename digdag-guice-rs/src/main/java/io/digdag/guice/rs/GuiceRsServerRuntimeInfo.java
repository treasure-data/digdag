package io.digdag.guice.rs;

import java.net.InetSocketAddress;
import java.util.List;

public interface GuiceRsServerRuntimeInfo
{
    static final String LISTEN_ADDRESS_NAME_ATTRIBUTE = "io.digdag.guice.rs.server.ListenAddress.name";

    interface ListenAddress
    {
        String getName();

        InetSocketAddress getSocketAddress();
    }

    List<ListenAddress> getListenAddresses();
}

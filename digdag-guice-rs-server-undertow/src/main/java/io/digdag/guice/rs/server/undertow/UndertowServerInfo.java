package io.digdag.guice.rs.server.undertow;

import java.net.InetSocketAddress;
import java.util.List;

public interface UndertowServerInfo
{
    boolean isStarted();

    List<InetSocketAddress> getLocalAddresses();

    List<InetSocketAddress> getLocalAdminAddresses();
}

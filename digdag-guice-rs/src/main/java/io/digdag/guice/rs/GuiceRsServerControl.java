package io.digdag.guice.rs;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public interface GuiceRsServerControl
{
    public void stop();

    public void destroy();

    Map<String, List<InetSocketAddress>> getListenAddresses();
}

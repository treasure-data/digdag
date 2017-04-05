package io.digdag.guice.rs.server.undertow;

import io.digdag.guice.rs.GuiceRsServerRuntimeInfo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import static java.util.Collections.unmodifiableList;

public class UndertowServerRuntimeInfo
        implements GuiceRsServerRuntimeInfo
{
    private final List<ListenAddress> addresses;

    public UndertowServerRuntimeInfo()
    {
        this.addresses = new ArrayList<>();
    }

    void addListenAddress(final String name, final InetSocketAddress socketAddress)
    {
        addresses.add(
                new GuiceRsServerRuntimeInfo.ListenAddress()
                {
                    @Override
                    public String getName()
                    {
                        return name;
                    }

                    @Override
                    public InetSocketAddress getSocketAddress()
                    {
                        return socketAddress;
                    }
                });
    }

    @Override
    public List<ListenAddress> getListenAddresses()
    {
        return unmodifiableList(addresses);
    }
}

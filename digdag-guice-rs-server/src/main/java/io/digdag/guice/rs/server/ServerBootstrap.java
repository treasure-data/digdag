package io.digdag.guice.rs.server;

import org.embulk.guice.Bootstrap;

import java.net.InetSocketAddress;
import java.util.List;

public interface ServerBootstrap
{
    Bootstrap bootstrap();
}

package io.digdag.guice.rs.server.undertow;

import com.google.common.base.Optional;

public interface UndertowServerConfig
{
    int getPort();

    String getBind();

    Optional<String> getAccessLogPath();

    String getAccessLogPattern();

    Optional<Integer> getHttpIoThreads();

    Optional<Integer> getHttpWorkerThreads();

    Optional<Integer> getJmxPort();
}

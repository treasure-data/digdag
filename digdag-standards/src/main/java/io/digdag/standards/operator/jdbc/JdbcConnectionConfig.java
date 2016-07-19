package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface JdbcConnectionConfig
{
    String database();
    Optional<String> schema();
    boolean ssl();
    String host();
    int port();
    String user();
    Optional<String> password();
    Optional<Integer> fetchSize();
    Optional<Integer> loginTimeout();
    Optional<Integer> connectionTimeout();
    Optional<Integer> socketTimeout();
}

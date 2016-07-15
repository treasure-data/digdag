package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.util.List;

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
    String queryId();
    Optional<String> destTable();
    Optional<List<String>> uniqKeys();
    Optional<String> statusTable();
}

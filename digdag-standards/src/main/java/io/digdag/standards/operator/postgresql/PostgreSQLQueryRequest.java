package io.digdag.standards.operator.postgresql;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface PostgreSQLQueryRequest
{
    String database();
    Optional<String> schema();
    String query();
    boolean ssl();
    String host();
    int port();
    String user();
    Optional<String> password();
}

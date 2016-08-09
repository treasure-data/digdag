package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface TableReference
{
    Optional<String> getSchema();

    String getName();

    static TableReference of(String name)
    {
        return ImmutableTableReference.builder()
            .schema(Optional.absent())
            .name(name)
            .build();
    }

    static TableReference of(String schema, String name)
    {
        return ImmutableTableReference.builder()
            .schema(Optional.of(schema))
            .name(name)
            .build();
    }

    @JsonCreator
    static TableReference parse(String expr)
    {
        int id = expr.indexOf('.');
        if (id >= 0) {
            return of(expr.substring(0, id), expr.substring(id + 1));
        }
        else {
            return of(expr);
        }
    }
}

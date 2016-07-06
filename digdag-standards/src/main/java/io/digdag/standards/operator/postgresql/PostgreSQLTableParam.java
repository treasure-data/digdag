package io.digdag.standards.operator.postgresql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;

public class PostgreSQLTableParam
{
    private final Optional<String> schema;
    private final String table;

    private PostgreSQLTableParam(Optional<String> schema, String table)
    {
        this.schema = schema;
        this.table = table;
    }

    @JsonCreator
    public static PostgreSQLTableParam parse(String expr)
    {
        int id = expr.indexOf('.');
        if (id >= 0) {
            return new PostgreSQLTableParam(Optional.of(expr.substring(0, id)), expr.substring(id + 1));
        }
        else {
            return new PostgreSQLTableParam(Optional.absent(), expr);
        }
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    @Override
    @JsonValue
    public String toString()
    {
        if (schema.isPresent()) {
            return schema.get() + '.' + table;
        }
        else {
            return table;
        }
    }
}

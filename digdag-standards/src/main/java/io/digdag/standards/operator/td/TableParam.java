package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;

public class TableParam
{
    private final Optional<String> database;
    private final String table;

    private TableParam(Optional<String> database, String table)
    {
        this.database = database;
        this.table = table;
    }

    @JsonCreator
    public static TableParam parse(String expr)
    {
        int id = expr.indexOf('.');
        if (id >= 0) {
            return new TableParam(Optional.of(expr.substring(0, id)), expr.substring(id + 1));
        }
        else {
            return new TableParam(Optional.absent(), expr);
        }
    }

    public Optional<String> getDatabase()
    {
        return database;
    }

    public String getTable()
    {
        return table;
    }

    @Override
    @JsonValue
    public String toString()
    {
        if (database.isPresent()) {
            return database.get() + '.' + table;
        }
        else {
            return table;
        }
    }
}

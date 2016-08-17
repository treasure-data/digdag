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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableParam that = (TableParam) o;

        if (database != null ? !database.equals(that.database) : that.database != null) {
            return false;
        }
        return table != null ? table.equals(that.table) : that.table == null;
    }

    @Override
    public int hashCode()
    {
        int result = database != null ? database.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        return result;
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

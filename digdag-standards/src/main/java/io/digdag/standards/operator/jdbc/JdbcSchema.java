package io.digdag.standards.operator.jdbc;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;

public class JdbcSchema
{
    private List<JdbcColumn> columns;

    @JsonCreator
    public JdbcSchema(List<JdbcColumn> columns)
    {
        this.columns = columns;
    }

    @JsonValue
    public List<JdbcColumn> getColumns()
    {
        return columns;
    }

    public int getCount()
    {
        return columns.size();
    }

    public JdbcColumn getColumn(int i)
    {
        return columns.get(i);
    }

    public String getColumnName(int i)
    {
        return columns.get(i).getName();
    }

    public Optional<JdbcColumn> findByNameIgnoreCase(String columnName) {
        for(JdbcColumn col : columns) {
            if(col != null && col.getName().compareToIgnoreCase(columnName) == 0) {
                return Optional.of(col);
            }
        }
        return Optional.absent();
    }

    @Override
    public String toString()
    {
        return "JdbcSchema{" +
                "columns=" + columns +
                '}';
    }
}

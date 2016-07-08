package io.digdag.standards.operator.jdbc;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

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

    @Override
    public String toString()
    {
        return "JdbcSchema{" +
                "columns=" + columns +
                '}';
    }
}

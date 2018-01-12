package io.digdag.standards.operator.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import com.google.common.collect.ImmutableList;

public abstract class AbstractJdbcResultSet
    implements JdbcResultSet
{
    private final ResultSet resultSet;
    private final List<String> columnNames;

    protected AbstractJdbcResultSet(ResultSet resultSet)
    {
        this.resultSet = resultSet;
        try {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();

            ImmutableList.Builder<String> names = ImmutableList.builder();
            for (int i=0; i < columnCount; i++) {
                names.add(meta.getColumnLabel(i + 1));  // JDBC column index begins from 1
            }
            this.columnNames = names.build();
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to fetch result metadata", ex);
        }
    }

    @Override
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public List<Object> next()
    {
        try {
            if (!resultSet.next()) {
                return null;
            }
            return getObjects();
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to fetch next rows", ex);
        }
    }

    @Override
    public boolean skip()
    {
        try {
            return resultSet.next();
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to fetch next rows", ex);
        }
    }

    private List<Object> getObjects() throws SQLException
    {
        List<Object> results = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            Object raw = resultSet.getObject(i + 1);  // JDBC column index begins from 1
            results.add(serializableObject(raw));
        }
        return results;
    }

    protected abstract Object serializableObject(Object raw)
        throws SQLException;
}

package io.digdag.standards.operator.jdbc;

import java.util.List;
import java.util.Arrays;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import com.google.common.collect.ImmutableList;

public abstract class AbstractJdbcResultSet
    implements JdbcResultSet
{
    protected final ResultSet resultSet;
    private final Object[] results;
    private final List<Object> list;
    private final List<String> columnNames;

    public AbstractJdbcResultSet(ResultSet resultSet)
    {
        this.resultSet = resultSet;
        try {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();
            this.results = new Object[columnCount];
            this.list = Arrays.asList(results);

            ImmutableList.Builder<String> names = ImmutableList.builder();
            for (int i=0; i < columnCount; i++) {
                names.add(meta.getColumnLabel(i + 1));  // JDBC column index begins from 1
            }
            this.columnNames = names.build();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Failed to fetch result metadata", ex);
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
            getObjects(results);
            return Arrays.asList(results);
        }
        catch (SQLException ex) {
            throw new RuntimeException("Failed to fetch next rows", ex);
        }
    }

    protected void getObjects(Object[] results) throws SQLException
    {
        for (int i=0; i < results.length; i++) {
            Object raw = resultSet.getObject(i + 1);  // JDBC column index begins from 1
            results[i] = serializableObject(raw);
        }
    }

    protected abstract Object serializableObject(Object raw)
        throws SQLException;
}

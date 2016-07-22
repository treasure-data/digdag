package io.digdag.standards.operator.jdbc;

import java.util.List;

public interface JdbcResultSet
{
    List<String> getColumnNames();

    List<Object> next();
}

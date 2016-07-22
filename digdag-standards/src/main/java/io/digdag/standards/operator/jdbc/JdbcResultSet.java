package io.digdag.standards.operator.jdbc;

import java.util.List;
import java.sql.SQLException;

public interface JdbcResultSet
{
    List<String> getColumnNames();

    List<Object> next();
}

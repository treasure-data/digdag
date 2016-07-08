package io.digdag.standards.operator.jdbc;

import java.util.List;

public interface QueryResultHandler
{
    void before();

    void schema(JdbcSchema schema);

    void handleRow(List<Object> row);

    void after();
}

package io.digdag.standards.operator.pg;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import io.digdag.standards.operator.jdbc.AbstractJdbcResultSet;

public class PgResultSet
    extends AbstractJdbcResultSet
{
    public PgResultSet(ResultSet resultSet)
    {
        super(resultSet);
    }

    @Override
    protected Object serializableObject(Object raw)
    {
        // TODO add more conversion logics here. postgresql jdbc may return objects that are not serializable using Jackson.
        return raw;
    }
}

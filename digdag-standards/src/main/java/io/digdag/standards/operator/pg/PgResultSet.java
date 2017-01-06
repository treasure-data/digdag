package io.digdag.standards.operator.pg;

import java.sql.ResultSet;
import io.digdag.standards.operator.jdbc.AbstractJdbcResultSet;

class PgResultSet
    extends AbstractJdbcResultSet
{
    PgResultSet(ResultSet resultSet)
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

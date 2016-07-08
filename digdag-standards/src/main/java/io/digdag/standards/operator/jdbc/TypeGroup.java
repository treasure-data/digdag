package io.digdag.standards.operator.jdbc;

import java.sql.Types;

public enum TypeGroup
{
    INT,
    LONG,
    BIG_DECIMAL,
    FLOAT,
    BOOLEAN,
    STRING,
    BYTES,
    TIMESTAMP,
    ARRAY,
    MAP,
    NULL;

    public static TypeGroup fromSqlType(int sqlType)
    {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return INT;

            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return FLOAT;

            case Types.BOOLEAN:
            case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
                return BOOLEAN;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return STRING;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BYTES;

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return TIMESTAMP;

            case Types.NULL:
                return NULL;

            case Types.NUMERIC:
            case Types.DECIMAL:
                return BIG_DECIMAL;

            case Types.ARRAY:
                return ARRAY;

            case Types.STRUCT:
                return MAP;

            case Types.REF:
            case Types.DATALINK:
            case Types.SQLXML: // XML
            case Types.ROWID:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            default:
                // TODO: Use a proper exception
                throw new RuntimeException("Unsupported type: " + sqlType);
        }
    }
}

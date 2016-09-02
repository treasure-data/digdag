package io.digdag.core.database.migrate;

import io.digdag.core.database.DatabaseConfig;

import java.util.List;
import java.util.ArrayList;

public class CreateTableBuilder
{
    private final String databaseType;
    private final String name;
    private final List<String> columns = new ArrayList<>();

    CreateTableBuilder(String databaseType, String name)
    {
        this.databaseType = databaseType;
        this.name = name;
    }

    private boolean isPostgres()
    {
        return DatabaseConfig.isPostgres(databaseType);
    }

    public CreateTableBuilder add(String column, String typeAndOptions)
    {
        columns.add(column + " " + typeAndOptions);
        return this;
    }

    public CreateTableBuilder addIntId(String column)
    {
        if (isPostgres()) {
            return add(column, "serial primary key");
        }
        else {
            return add(column, "int primary key AUTO_INCREMENT");
        }
    }

    public CreateTableBuilder addIntIdNoAutoIncrement(String column, String options)
    {
        if (isPostgres()) {
            return add(column, "serial primary key " + options);
        }
        else {
            return add(column, "int primary key AUTO_INCREMENT " + options);
        }
    }

    public CreateTableBuilder addLongId(String column)
    {
        if (isPostgres()) {
            return add(column, "bigserial primary key");
        }
        else {
            return add(column, "bigint primary key AUTO_INCREMENT");
        }
    }

    public CreateTableBuilder addLongIdNoAutoIncrement(String column, String options)
    {
        return add(column, "bigint primary key " + options);
    }

    public CreateTableBuilder addShort(String column, String options)
    {
        return add(column, "smallint " + options);
    }

    public CreateTableBuilder addInt(String column, String options)
    {
        return add(column, "int " + options);
    }

    public CreateTableBuilder addLong(String column, String options)
    {
        return add(column, "bigint " + options);
    }

    public CreateTableBuilder addUuid(String column, String options)
    {
        return add(column, "uuid " + options);
    }

    public CreateTableBuilder addString(String column, String options)
    {
        if (isPostgres()) {
            return add(column, "text " + options);
        }
        else {
            return add(column, "varchar(255) " + options);
        }
    }

    public CreateTableBuilder addMediumText(String column, String options)
    {
        return add(column, "text " + options);
    }

    public CreateTableBuilder addLongText(String column, String options)
    {
        return add(column, "text " + options);
    }

    public CreateTableBuilder addBinary(String column, String options)
    {
        if (isPostgres()) {
            return add(column, "bytea " + options);
        }
        else {
            return add(column, "varbinary(255) " + options);
        }
    }

    public CreateTableBuilder addLongBinary(String column, String options)
    {
        if (isPostgres()) {
            return add(column, "bytea " + options);
        }
        else {
            return add(column, "blob " + options);
        }
    }

    public CreateTableBuilder addTimestamp(String column, String options)
    {
        if (isPostgres()) {
            return add(column, "timestamp with time zone " + options);
        }
        else {
            return add(column, "timestamp " + options);
        }
    }

    public String build()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + name + " (\n");
        for (int i=0; i < columns.size(); i++) {
            sb.append("  ");
            sb.append(columns.get(i));
            if (i + 1 < columns.size()) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}

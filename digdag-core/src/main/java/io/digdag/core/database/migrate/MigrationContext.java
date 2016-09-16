package io.digdag.core.database.migrate;

import io.digdag.core.database.DatabaseConfig;

public class MigrationContext
{
    private final String databaseType;

    public MigrationContext(String databaseType)
    {
        this.databaseType = databaseType;
    }

    public boolean isPostgres()
    {
        return DatabaseConfig.isPostgres(databaseType);
    }

    public CreateTableBuilder newCreateTableBuilder(String tableName)
    {
        return new CreateTableBuilder(databaseType, tableName);
    }
}

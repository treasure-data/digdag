package io.digdag.core.database;

import com.google.common.base.Throwables;
import com.google.inject.Provider;
import org.skife.jdbi.v2.DBI;
import static io.digdag.core.database.DatabaseTestingUtils.*;

public class DatabaseFactory
        implements AutoCloseable, Provider<DBI>
{
    private final DBI dbi;
    private final AutoCloseable closeable;
    private final DatabaseConfig config;

    public DatabaseFactory(DBI dbi, AutoCloseable closeable, DatabaseConfig config)
    {
        this.dbi = dbi;
        this.closeable = closeable;
        this.config = config;
    }

    public DBI get()
    {
        return dbi;
    }

    public DatabaseRepositoryStoreManager getRepositoryStoreManager()
    {
        return new DatabaseRepositoryStoreManager(dbi, createConfigMapper(), config);
    }

    public void close()
    {
        try {
            closeable.close();
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
}

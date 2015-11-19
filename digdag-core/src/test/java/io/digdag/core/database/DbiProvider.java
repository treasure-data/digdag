package io.digdag.core.database;

import com.google.common.base.Throwables;
import com.google.inject.Provider;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

public class DbiProvider
        implements AutoCloseable, Provider<IDBI>
{
    private final IDBI dbi;
    private final AutoCloseable closeable;

    public DbiProvider(IDBI dbi, AutoCloseable closeable)
    {
        this.dbi = dbi;
        this.closeable = closeable;
    }

    public IDBI get()
    {
        return dbi;
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

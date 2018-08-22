package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.skife.jdbi.v2.DBI;

import javax.sql.DataSource;

public class UserDbiProvider
        implements Provider<DBI>
{
    private final DataSource ds;

    @Inject
    public UserDbiProvider(@Named("param_server.database") DataSource ds)
    {
        this.ds = ds;
    }

    @Override
    public DBI get()
    {
        return new DBI(ds);
    }
}

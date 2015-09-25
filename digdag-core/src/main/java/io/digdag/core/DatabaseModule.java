package io.digdag.core;

import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import javax.sql.DataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.h2.jdbcx.JdbcConnectionPool;

public class DatabaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // TODO DBI provider
        DataSource ds = JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
        DBI dbi = new DBI(ds);

        binder.bind(IDBI.class).toProvider(() -> dbi);
        binder.bind(SessionStoreManager.class).to(DatabaseSessionStoreManager.class).in(Scopes.SINGLETON);
    }
}

package io.digdag.core.database;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.h2.H2DatabasePlugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.sql.DataSource;

public class DatabaseHelper
{
    public static Jdbi createJdbi(DataSource ds)
    {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.installPlugin(new SqlObjectPlugin());
        if (ds.getClass().getCanonicalName().startsWith("org.h2")) {
            jdbi.installPlugin(new H2DatabasePlugin());
        }
        else {
            jdbi.installPlugin(new PostgresPlugin());
        }
        return jdbi;
    }
}

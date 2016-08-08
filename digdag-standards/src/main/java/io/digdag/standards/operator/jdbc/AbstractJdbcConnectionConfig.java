package io.digdag.standards.operator.jdbc;

import java.util.Properties;
import java.time.Duration;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.util.DurationParam;
import static java.util.Locale.ENGLISH;

public abstract class AbstractJdbcConnectionConfig
{
    public abstract String host();

    public abstract int port();

    public abstract boolean ssl();

    public abstract String user();

    public abstract Optional<String> password();

    public abstract String database();

    public abstract DurationParam connectTimeout();

    public abstract DurationParam socketTimeout();

    public abstract String jdbcProtocolName();

    public abstract String jdbcDriverName();

    public abstract Properties buildProperties();

    public String url()
    {
        return String.format(ENGLISH, "jdbc:%s://%s:%d/%s", jdbcProtocolName(), host(), port(), database());
    }

    public Connection openConnection()
    {
        try {
            Class.forName(jdbcDriverName());
        }
        catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }

        try {
            return DriverManager.getConnection(url(), buildProperties());
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to connect to the database", ex);
        }
    }
}

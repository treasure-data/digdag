package io.digdag.standards.operator.jdbc;

import java.util.Properties;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.SecretProvider;
import io.digdag.util.DurationParam;
import static java.util.Locale.ENGLISH;

public abstract class AbstractJdbcConnectionConfig
{
    protected static Optional<String> getPassword(SecretProvider secrets, Config params)
    {
        Optional<String> passwordOverrideKey = params.getOptional("password_override", String.class);
        if (passwordOverrideKey.isPresent()) {
            // When `password_override` is specified, the key needs to exist
            return Optional.of(secrets.getSecret(passwordOverrideKey.get()));
        }
        else {
            return secrets.getSecretOptional("password");
        }
    }

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
            throw ThrowablesUtil.propagate(e);
        }

        try {
            return DriverManager.getConnection(url(), buildProperties());
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to connect to the database", ex);
        }
    }
}

package io.digdag.standards.operator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import com.google.common.base.Throwables;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

public abstract class JdbcConnection
        implements AutoCloseable
{
    protected static final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    protected final JdbcConnectionConfig config;
    protected final Connection connection;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;

    protected abstract String getDriverClassName();

    protected abstract String getProtocolName();

    public JdbcConnection(JdbcConnectionConfig config)
            throws SQLException
    {
        this.config = config;

        String url = String.format(Locale.ENGLISH, "jdbc:%s://%s:%d/%s", getProtocolName(), config.host(), config.port(), config.database());
        try {
            Class.forName(getDriverClassName());
        }
        catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }

        Properties props = new Properties();
        props.setProperty("user", config.user());
        if (config.password().isPresent()) {
            props.setProperty("password", config.password().get());
        }
        if (config.schema().isPresent()) {
            props.setProperty("currentSchema", config.schema().get());
        }
        props.setProperty("loginTimeout", String.valueOf(config.loginTimeout().or(30)));
        props.setProperty("connectTimeout", String.valueOf(config.loginTimeout().or(30)));
        props.setProperty("socketTimeout", String.valueOf(config.loginTimeout().or(1800)));
        props.setProperty("tcpKeepAlive", "true");
        props.setProperty("ssl", String.valueOf(config.ssl()));
        props.setProperty("applicationName", "digdag");

        this.connection = DriverManager.getConnection(url, props);
        this.databaseMetaData = connection.getMetaData();
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        connection.setAutoCommit(true);
    }

    public Connection getConnection()
    {
        return connection;
    }

    @Override
    public void close() throws SQLException
    {
        logger.info("Closing connection");
        connection.close();
    }
}

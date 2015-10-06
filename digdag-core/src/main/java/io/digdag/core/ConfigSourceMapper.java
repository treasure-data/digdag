package io.digdag.core;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.ResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

public class ConfigSourceMapper
{
    private final ObjectMapper jsonTreeMapper;
    private final ConfigSourceFactory cf;

    @Inject
    public ConfigSourceMapper(ConfigSourceFactory cf)
    {
        this.jsonTreeMapper = new ObjectMapper();
        this.cf = cf;
    }

    public ConfigSourceArgumentFactory getArgumentFactory()
    {
        return new ConfigSourceArgumentFactory();
    }

    public Optional<ConfigSource> fromResultSet(ResultSet rs, String column)
            throws SQLException
    {
        String text = rs.getString(column);
        if (rs.wasNull()) {
            return Optional.absent();
        }
        else {
            return Optional.of(fromText(text));
        }
    }

    public ConfigSource fromResultSetOrEmpty(ResultSet rs, String column)
            throws SQLException
    {
        String text = rs.getString(column);
        if (rs.wasNull()) {
            return cf.create();
        }
        else {
            return fromText(text);
        }
    }

    private ConfigSource fromText(String text)
    {
        try {
            JsonNode node = jsonTreeMapper.readTree(text);
            Preconditions.checkState(node instanceof ObjectNode, "Stored ConfigSource must be an object");
            return cf.create((ObjectNode) node);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private String toText(ConfigSource config)
    {
        if (config.isEmpty()) {
            return null;
        }
        try {
            return jsonTreeMapper.writeValueAsString(config);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public class ConfigSourceArgumentFactory
            implements ArgumentFactory<ConfigSource>
    {
        @Override
        public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx)
        {
            return value instanceof ConfigSource;
        }

        @Override
        public Argument build(Class<?> expectedType, ConfigSource value, StatementContext ctx)
        {
            return new ConfigSourceArgument(value);
        }
    }

    public class ConfigSourceArgument
            implements Argument
    {
        private final ConfigSource config;

        public ConfigSourceArgument(ConfigSource config)
        {
            this.config = config;
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            if (config == null) {
                statement.setNull(position, Types.CLOB);
            }
            else {
                statement.setString(position, toText(config));
            }
        }

        @Override
        public String toString()
        {
            return String.valueOf(config);
        }
    }
}

package io.digdag.core.database;

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
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

class ConfigMapper
{
    private final ObjectMapper jsonTreeMapper;
    private final ConfigFactory cf;

    @Inject
    public ConfigMapper(ConfigFactory cf)
    {
        this.jsonTreeMapper = new ObjectMapper();
        this.cf = cf;
    }

    public ConfigArgumentFactory getArgumentFactory()
    {
        return new ConfigArgumentFactory();
    }

    public Optional<Config> fromResultSet(ResultSet rs, String column)
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

    public Config fromResultSetOrEmpty(ResultSet rs, String column)
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

    private Config fromText(String text)
    {
        try {
            JsonNode node = jsonTreeMapper.readTree(text);
            Preconditions.checkState(node instanceof ObjectNode, "Stored Config must be an object");
            return cf.create((ObjectNode) node);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public String toText(Config config)
    {
        try {
            return jsonTreeMapper.writeValueAsString(config);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public String toBinding(Config config)
    {
        if (config == null) {
            return null;
        }
        else {
            String text = toText(config);
            if ("{}".equals(text)) {
                return null;
            }
            else {
                return text;
            }
        }
    }

    public class ConfigArgumentFactory
            implements ArgumentFactory<Config>
    {
        @Override
        public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx)
        {
            return value instanceof Config;
        }

        @Override
        public Argument build(Class<?> expectedType, Config value, StatementContext ctx)
        {
            return new ConfigArgument(value);
        }
    }

    public class ConfigArgument
            implements Argument
    {
        private final Config config;

        public ConfigArgument(Config config)
        {
            this.config = config;
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            String text = toBinding(config);
            if (text == null) {
                statement.setNull(position, Types.CLOB);
            }
            else {
                statement.setString(position, text);
            }
        }

        @Override
        public String toString()
        {
            return String.valueOf(config);
        }
    }
}

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
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

public class JsonMapper <T>
{
    private final ObjectMapper mapper;
    private final Class<T> type;

    public JsonMapper(ObjectMapper mapper, Class<T> type)
    {
        this.mapper = mapper;
        this.type = type;
    }

    public JsonArgumentFactory getArgumentFactory()
    {
        return new JsonArgumentFactory();
    }

    public T fromResultSet(ResultSet rs, String column)
            throws SQLException
    {
        String text = rs.getString(column);
        if (rs.wasNull()) {
            return fromText("{}");
        }
        else {
            return fromText(text);
        }
    }

    public Optional<T> fromNullableResultSet(ResultSet rs, String column)
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

    private T fromText(String text)
    {
        try {
            return mapper.readValue(text, type);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private String toText(T value)
    {
        try {
            return mapper.writeValueAsString(value);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public class JsonArgumentFactory
            implements ArgumentFactory<T>
    {
        @Override
        public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx)
        {
            return type.isInstance(value);
        }

        @Override
        public Argument build(Class<?> expectedType, T value, StatementContext ctx)
        {
            return new JsonArgument(value);
        }
    }

    public class JsonArgument
            implements Argument
    {
        private final T value;

        public JsonArgument(T value)
        {
            this.value = value;
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            if (value == null) {
                statement.setNull(position, Types.CLOB);
            }
            else {
                String text = toText(value);
                statement.setString(position, text);
            }
        }

        @Override
        public String toString()
        {
            return String.valueOf(value);
        }
    }
}

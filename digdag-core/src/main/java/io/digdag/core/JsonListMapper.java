package io.digdag.core;

import java.util.List;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.ResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

public class JsonListMapper
{
    private final ObjectMapper mapper;

    @Inject
    public JsonListMapper(ObjectMapper mapper)
    {
        this.mapper = mapper;
    }

    public JsonListArgumentFactory<Object> getArgumentFactory()
    {
        return new JsonListArgumentFactory<Object>();
    }

    public <E> List<E> fromResultSet(ResultSet rs, String column, Class<E> elementType)
            throws SQLException
    {
        return fromText(rs.getString(column), elementType);
    }

    @SuppressWarnings("unchecked")
    private <E> List<E> fromText(String text, Class<E> elementType)
    {
        try {
            return (List<E>) mapper.readValue(text, mapper.getTypeFactory().constructParametrizedType(List.class, List.class, elementType));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private String toText(List<?> list)
    {
        try {
            return mapper.writeValueAsString(list);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public class JsonListArgumentFactory <E>
            implements ArgumentFactory<List<E>>
    {
        @Override
        public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx)
        {
            return value instanceof List;
        }

        @Override
        public Argument build(Class<?> expectedType, List<E> value, StatementContext ctx)
        {
            return new JsonListArgument<E>(value);
        }
    }

    public class JsonListArgument <E>
            implements Argument
    {
        private final List<E> list;

        public JsonListArgument(List<E> list)
        {
            this.list = list;
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            if (list == null) {
                statement.setNull(position, Types.CLOB);
            }
            else {
                statement.setString(position, toText(list));
            }
        }

        @Override
        public String toString()
        {
            return String.valueOf(list);
        }
    }
}

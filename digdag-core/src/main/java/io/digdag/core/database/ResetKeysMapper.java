package io.digdag.core.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.digdag.core.session.ConfigKey;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

class ResetKeysMapper
{
    private final ObjectMapper jsonTreeMapper = new ObjectMapper();

    public List<ConfigKey> fromResultSetOrEmpty(ResultSet rs, String column)
        throws SQLException
    {
        String text = rs.getString(column);
        if (rs.wasNull()) {
            return ImmutableList.of();
        }
        else {
            return fromText(text);
        }
    }

    @SuppressWarnings("unchecked")
    private List<ConfigKey> fromText(String text)
    {
        try {
            return (List<ConfigKey>) jsonTreeMapper.readValue(text, jsonTreeMapper.getTypeFactory().constructParametrizedType(List.class, List.class, ConfigKey.class));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private String toText(List<ConfigKey> keys)
    {
        try {
            return jsonTreeMapper.writeValueAsString(keys);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public String toBinding(List<ConfigKey> keys)
    {
        if (keys.isEmpty()) {
            return null;
        }
        else {
            String text = toText(keys);
            if ("[]".equals(text)) {
                return null;
            }
            else {
                return text;
            }
        }
    }
}

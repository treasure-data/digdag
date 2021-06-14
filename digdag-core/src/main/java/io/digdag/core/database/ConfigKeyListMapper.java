package io.digdag.core.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.ConfigKey;
import io.digdag.commons.ThrowablesUtil;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class ConfigKeyListMapper
{
    private final ObjectMapper jsonTreeMapper = new ObjectMapper();

    public List<ConfigKey> fromResultSetOrEmpty(ResultSet rs, String column)
        throws SQLException
    {
        String text = rs.getString(column);
        if (rs.wasNull()) {
            return new ArrayList<>();
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
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private String toText(List<ConfigKey> keys)
    {
        try {
            return jsonTreeMapper.writeValueAsString(keys);
        }
        catch (IOException ex) {
            throw ThrowablesUtil.propagate(ex);
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

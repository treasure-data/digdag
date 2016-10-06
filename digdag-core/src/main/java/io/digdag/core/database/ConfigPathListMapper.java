package io.digdag.core.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigPath;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

class ConfigPathListMapper
{
    private final ObjectMapper jsonTreeMapper = new ObjectMapper();

    public List<ConfigPath> fromResultSetOrEmpty(ResultSet rs, String column)
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
    private List<ConfigPath> fromText(String text)
    {
        try {
            return (List<ConfigPath>) jsonTreeMapper.readValue(text, jsonTreeMapper.getTypeFactory().constructParametrizedType(List.class, List.class, ConfigPath.class));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private String toText(List<ConfigPath> paths)
    {
        try {
            return jsonTreeMapper.writeValueAsString(paths);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public String toBinding(List<ConfigPath> paths)
    {
        if (paths.isEmpty()) {
            return null;
        }
        else {
            String text = toText(paths);
            if ("[]".equals(text)) {
                return null;
            }
            else {
                return text;
            }
        }
    }
}

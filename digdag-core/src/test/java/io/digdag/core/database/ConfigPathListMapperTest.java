package io.digdag.core.database;

import io.digdag.client.config.ConfigPath;
import java.sql.ResultSet;
import java.util.List;
import org.h2.tools.SimpleResultSet;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ConfigPathListMapperTest
{
    private ConfigPathListMapper mapper;

    @Before
    public void setUp()
    {
        mapper = new ConfigPathListMapper();
    }

    @Test
    public void verifyTextFormat()
            throws Exception
    {
        List<ConfigPath> list = asList(ConfigPath.of("a", "b"), ConfigPath.of("c", "d"));
        String text = "[\"/a/b\",\"/c/d\"]";
        assertThat(
                mapper.toBinding(list),
                is(text));
        assertThat(
                mapper.fromResultSetOrEmpty(mockResultSet(text), "mock"),
                is(list));
    }

    @Test
    public void emptyToNull()
            throws Exception
    {
        assertNull(mapper.toBinding(asList()));
    }

    @Test
    public void nullToEmpty()
            throws Exception
    {
        assertThat(
                mapper.fromResultSetOrEmpty(mockResultSet(null), "mock"),
                is(asList()));
        assertThat(
                mapper.fromResultSetOrEmpty(mockResultSet("[]"), "mock"),
                is(asList()));
    }

    private static ResultSet mockResultSet(final String text)
    {
        return new SimpleResultSet()
        {
            @Override
            public String getString(String column)
            {
                return text;
            }

            @Override
            public boolean wasNull()
            {
                return text == null;
            }
        };
    }
}

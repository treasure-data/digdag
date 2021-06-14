package io.digdag.core.database;

import io.digdag.client.config.ConfigKey;
import java.sql.ResultSet;
import java.util.List;
import org.h2.tools.SimpleResultSet;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigKeyListMapperTest
{
    private ConfigKeyListMapper mapper;

    @Before
    public void setUp()
    {
        mapper = new ConfigKeyListMapper();
    }

    @Test
    public void verifyTextFormat()
            throws Exception
    {
        List<ConfigKey> list = asList(ConfigKey.of("a", "b"), ConfigKey.of("c", "d"));
        String text = "[\"a.b\",\"c.d\"]";
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

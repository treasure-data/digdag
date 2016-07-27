package io.digdag.standards.operator.pg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class PgConnectionConfigTest
{
    private final ConfigBuilder configBuilder = new ConfigBuilder();
    private PgConnectionConfig connConfigWithDefaultValue;
    private PgConnectionConfig connConfigWithCustomValue;

    @Before
    public void setUp()
            throws IOException
    {
        {
            Map<String, String> configInput = ImmutableMap.of(
                    "host", "foobar0.org",
                    "user", "user0",
                    "database", "database0"
            );
            this.connConfigWithDefaultValue = PgConnectionConfig.configure(configBuilder.createConfig(configInput));
        }

        {
            Map<String, String> configInput = ImmutableMap.<String, String>builder().
                    put("host", "foobar1.org").
                    put("port", "6543").
                    put("user", "user1").
                    put("password", "password1").
                    put("database", "database1").
                    put("ssl", "true").
                    put("connect_timeout", "15s").
                    put("socket_timeout", "12 m").
                    put("schema", "myschema").build();
            this.connConfigWithCustomValue = PgConnectionConfig.configure(configBuilder.createConfig(configInput));
        }
    }

    @Test
    public void jdbcDriverName()
    {
        assertThat(connConfigWithDefaultValue.jdbcDriverName(), is("org.postgresql.Driver"));
    }

    @Test
    public void jdbcProtocolName()
    {
        assertThat(connConfigWithDefaultValue.jdbcProtocolName(), is("postgresql"));
    }

    @Test
    public void url()
    {
        assertThat(connConfigWithDefaultValue.url(), is("jdbc:postgresql://foobar0.org:5432/testdb0"));
        assertThat(connConfigWithCustomValue.url(), is("jdbc:postgresql://foobar1.org:6543/testdb1"));
    }

    @Test
    public void buildProperties()
    {
        {
            Properties properties = connConfigWithDefaultValue.buildProperties();
            assertThat(properties.get("user"), is("user0"));
            assertThat(properties.get("password"), is(nullValue()));
            assertThat(properties.get("currentSchema"), is(nullValue()));
            assertThat(properties.get("loginTimeout"), is("30"));
            assertThat(properties.get("connectTimeout"), is("30"));
            assertThat(properties.get("socketTimeout"), is("1800"));
            assertThat(properties.get("tcpKeepAlive"), is("true"));
            assertThat(properties.get("ssl"), is(nullValue()));
            assertThat(properties.get("sslfactory"), is(nullValue()));
            assertThat(properties.get("applicationName"), is("digdag"));
        }

        {
            Properties properties = connConfigWithCustomValue.buildProperties();
            assertThat(properties.get("user"), is("user1"));
            assertThat(properties.get("password"), is("password1"));
            assertThat(properties.get("currentSchema"), is("myschema"));
            assertThat(properties.get("loginTimeout"), is("15"));
            assertThat(properties.get("connectTimeout"), is("15"));
            assertThat(properties.get("socketTimeout"), is("720"));
            assertThat(properties.get("tcpKeepAlive"), is("true"));
            assertThat(properties.get("ssl"), is("true"));
            assertThat(properties.get("sslfactory"), is("org.postgresql.ssl.NonValidatingFactory"));
            assertThat(properties.get("applicationName"), is("digdag"));
        }
    }
}
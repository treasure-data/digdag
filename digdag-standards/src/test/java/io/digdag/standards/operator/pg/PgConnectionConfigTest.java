package io.digdag.standards.operator.pg;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.digdag.standards.operator.jdbc.JdbcOpTestHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class PgConnectionConfigTest
{
    private final JdbcOpTestHelper jdbcOpTestHelper = new JdbcOpTestHelper();

    private PgConnectionConfig connConfigWithDefaultValue;
    private PgConnectionConfig connConfigWithCustomValue;
    private PgConnectionConfig connConfigWithDefaultValueFromSecrets;
    private PgConnectionConfig connConfigWithCustomValueFromSecrets;

    @Before
    public void setUp()
            throws IOException
    {
        Map<String, String> defaultConfigValues = ImmutableMap.of(
                "host", "foobar0.org",
                "user", "user0",
                "database", "database0"
        );
        Map<String, String> ignoredDefaultConfigValues = Maps.transformValues(defaultConfigValues, key -> "ignore");
        Map<String, String> customConfigValues = ImmutableMap.<String, String>builder().
                put("host", "foobar1.org").
                put("port", "6543").
                put("user", "user1").
                put("password", "password1").
                put("database", "database1").
                put("ssl", "true").
                put("connect_timeout", "15s").
                put("socket_timeout", "12 m").
                put("schema", "myschema").build();
        Map<String, String> ignoredCustomConfigValues = Maps.transformValues(customConfigValues, key -> "ignore");

        this.connConfigWithDefaultValue = PgConnectionConfig.configure(
                key -> Optional.absent(), jdbcOpTestHelper.createConfig(defaultConfigValues)
        );

        this.connConfigWithDefaultValueFromSecrets = PgConnectionConfig.configure(
                key -> Optional.fromNullable(defaultConfigValues.get(key)), jdbcOpTestHelper.createConfig(ignoredDefaultConfigValues)
        );

        this.connConfigWithCustomValue = PgConnectionConfig.configure(
                key -> key.equals("password") ? Optional.of("password1") : Optional.absent(), jdbcOpTestHelper.createConfig(customConfigValues)
        );

        this.connConfigWithCustomValueFromSecrets = PgConnectionConfig.configure(
                key -> Optional.fromNullable(customConfigValues.get(key)), jdbcOpTestHelper.createConfig(ignoredCustomConfigValues)
        );
    }

    @Test
    public void paramsVsSecretsEquals()
            throws Exception
    {
        assertThat(connConfigWithDefaultValue, is(connConfigWithDefaultValueFromSecrets));
        assertThat(connConfigWithCustomValue, is(connConfigWithCustomValueFromSecrets));
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
        assertThat(connConfigWithDefaultValue.url(), is("jdbc:postgresql://foobar0.org:5432/database0"));
        assertThat(connConfigWithDefaultValueFromSecrets.url(), is("jdbc:postgresql://foobar0.org:5432/database0"));
        assertThat(connConfigWithCustomValue.url(), is("jdbc:postgresql://foobar1.org:6543/database1"));
        assertThat(connConfigWithCustomValueFromSecrets.url(), is("jdbc:postgresql://foobar1.org:6543/database1"));
    }

    @Test
    public void buildProperties()
    {
        validateDefaultValueProperties(connConfigWithDefaultValue.buildProperties());
        validateDefaultValueProperties(connConfigWithDefaultValueFromSecrets.buildProperties());
        validateCustomValueProperties(connConfigWithCustomValue.buildProperties());
        validateCustomValueProperties(connConfigWithCustomValueFromSecrets.buildProperties());
    }

    private void validateCustomValueProperties(Properties properties)
    {
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

    private void validateDefaultValueProperties(Properties properties)
    {
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
}
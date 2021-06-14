package io.digdag.standards.operator.pg;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.standards.operator.jdbc.JdbcOpTestHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PgConnectionConfigTest
{
    private final JdbcOpTestHelper jdbcOpTestHelper = new JdbcOpTestHelper();

    private PgConnectionConfig connConfigWithDefaultValue;
    private PgConnectionConfig connConfigWithCustomValue;
    private PgConnectionConfig connConfigWithDefaultValueFromSecrets;
    private PgConnectionConfig connConfigWithCustomValueFromSecrets;
    private PgConnectionConfig connConfigWithOverriddenPassword;

    @Before
    public void setUp()
            throws IOException
    {
        // This map contains only minimum custom values to test default values
        Map<String, String> defaultConfigValues = ImmutableMap.of(
                "host", "foobar0.org",
                "user", "user0",
                "database", "database0"
        );

        this.connConfigWithDefaultValue = PgConnectionConfig.configure(
                key -> Optional.absent(), jdbcOpTestHelper.createConfig(defaultConfigValues)
        );

        // This map contains values that are all "ignore" so that we can detect if this value is used unexpectedly
        Map<String, String> ignoredDefaultConfigValues = Maps.transformValues(defaultConfigValues, key -> "ignore");

        this.connConfigWithDefaultValueFromSecrets = PgConnectionConfig.configure(
                key -> Optional.fromNullable(defaultConfigValues.get(key)), jdbcOpTestHelper.createConfig(ignoredDefaultConfigValues)
        );

        // This map contains whole custom values to test if custom values are used expectedly
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

        this.connConfigWithCustomValue = PgConnectionConfig.configure(
                key -> key.equals("password") ? Optional.of("password1") : Optional.absent(), jdbcOpTestHelper.createConfig(customConfigValues)
        );

        // This map contains values that are all "ignore" so that we can detect if this value is used unexpectedly
        Map<String, String> ignoredCustomConfigValues = Maps.transformValues(customConfigValues, key -> "ignore");

        this.connConfigWithCustomValueFromSecrets = PgConnectionConfig.configure(
                key -> Optional.fromNullable(customConfigValues.get(key)), jdbcOpTestHelper.createConfig(ignoredCustomConfigValues)
        );

        Map<String, String> configValuesWithOverriddenPassword = ImmutableMap.<String, String>builder()
                .putAll(customConfigValues)
                .put("another_db_password", "password2")
                .build();

        Map<String, String> configValuesUsingOverriddenPassword = ImmutableMap.<String, String>builder()
                .putAll(customConfigValues)
                .put("password_override", "another_db_password")
                .build();

        this.connConfigWithOverriddenPassword = PgConnectionConfig.configure(
                key -> Optional.fromNullable(configValuesWithOverriddenPassword.get(key)),
                jdbcOpTestHelper.createConfig(configValuesUsingOverriddenPassword)
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
        validateCustomValueProperties(connConfigWithCustomValue.buildProperties(), Optional.absent());
        validateCustomValueProperties(connConfigWithCustomValueFromSecrets.buildProperties(), Optional.absent());
    }

    private void validateCustomValueProperties(Properties properties, Optional expectedCustomPassword)
    {
        assertThat(properties.get("user"), is("user1"));
        assertThat(properties.get("password"), is(expectedCustomPassword.or("password1")));
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

    @Test
    public void configureWithOverriddenPassword()
    {
        validateCustomValueProperties(connConfigWithOverriddenPassword.buildProperties(), Optional.of("password2"));
    }

    @Test(expected = SecretNotFoundException.class)
    public void configureWithMissingOverriddenPassword()
            throws IOException
    {
        Map<String, String> configValues = ImmutableMap.<String, String>builder().
                put("host", "foobar1.org").
                put("port", "6543").
                put("user", "user1").
                put("password_override", "missing_db_password").
                put("database", "database1").build();

        PgConnectionConfig.configure(
                key -> key.equals("password") ? Optional.of("password1") : Optional.absent(),
                jdbcOpTestHelper.createConfig(configValues)
        );
    }
}

package io.digdag.standards.operator.pg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnectionConfig;
import io.digdag.util.DurationParam;
import org.immutables.value.Value;

import java.time.Duration;
import java.util.Properties;

import static io.digdag.standards.operator.jdbc.AbstractJdbcOperator.ParamName.*;

@Value.Immutable
public abstract class PgConnectionConfig
    extends AbstractJdbcConnectionConfig
{
    public abstract Optional<String> schema();

    @VisibleForTesting
    public static PgConnectionConfig configure(SecretProvider secrets, Config params)
    {
        return ImmutablePgConnectionConfig.builder()
                .host(secrets.getSecretOptional(HOST.get()).or(() -> params.get(HOST.get(), String.class)))
                .port(secrets.getSecretOptional(PORT.get()).transform(Integer::parseInt).or(() -> params.get(PORT.get(), int.class, 5432)))
                .user(secrets.getSecretOptional(USER.get()).or(() -> params.get(USER.get(), String.class)))
                .password(secrets.getSecretOptional(PASSWORD.get()))
                .database(secrets.getSecretOptional(DATABASE.get()).or(() -> params.get(DATABASE.get(), String.class)))
                .ssl(secrets.getSecretOptional(SSL.get()).transform(Boolean::parseBoolean).or(() -> params.get(SSL.get(), boolean.class, false)))
                .connectTimeout(secrets.getSecretOptional(CONNECT_TIMEOUT.get()).transform(DurationParam::parse).or(() ->
                        params.get(CONNECT_TIMEOUT.get(), DurationParam.class, DurationParam.of(Duration.ofSeconds(30)))))
                .socketTimeout(secrets.getSecretOptional(SOCKET_TIMEOUT.get()).transform(DurationParam::parse).or(() ->
                        params.get(SOCKET_TIMEOUT.get(), DurationParam.class, DurationParam.of(Duration.ofSeconds(1800)))))
                .schema(secrets.getSecretOptional(SCHEMA.get()).or(params.getOptional(SCHEMA.get(), String.class)))
                .build();
    }

    @Override
    public String jdbcDriverName()
    {
        return "org.postgresql.Driver";
    }

    @Override
    public String jdbcProtocolName()
    {
        return "postgresql";
    }

    @Override
    public Properties buildProperties()
    {
        Properties props = new Properties();

        props.setProperty("user", user());
        if (password().isPresent()) {
            props.setProperty("password", password().get());
        }
        if (schema().isPresent()) {
            props.setProperty("currentSchema", schema().get());
        }
        props.setProperty("loginTimeout", String.valueOf(connectTimeout().getDuration().getSeconds()));
        props.setProperty("connectTimeout", String.valueOf(connectTimeout().getDuration().getSeconds()));
        props.setProperty("socketTimeout", String.valueOf(socketTimeout().getDuration().getSeconds()));
        props.setProperty("tcpKeepAlive", "true");
        if (ssl()) {
            props.setProperty("ssl", "true");
            props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        }
        props.setProperty("applicationName", "digdag");

        return props;
    }

    @Override
    public String toString()
    {
        // Omit credentials in toString output
        return url();
    }
}

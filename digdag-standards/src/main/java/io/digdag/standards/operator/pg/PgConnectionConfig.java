package io.digdag.standards.operator.pg;

import java.util.Properties;
import java.time.Duration;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;
import io.digdag.util.DurationParam;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnectionConfig;

@Value.Immutable
public abstract class PgConnectionConfig
    extends AbstractJdbcConnectionConfig
{
    public abstract Optional<String> schema();

    static PgConnectionConfig configure(Config params)
    {
        return ImmutablePgConnectionConfig.builder()

            .host(params.get("host", String.class))
            .port(params.get("port", int.class, 5432))
            .user(params.get("user", String.class))
            .password(params.getOptional("password", String.class))
            .database(params.get("database", String.class))
            .ssl(params.get("ssl", boolean.class, false))
            .connectTimeout(params.get("connect_timeout", DurationParam.class,
                DurationParam.of(Duration.ofSeconds(30))))
            .socketTimeout(params.get("socket_timeout", DurationParam.class,
                DurationParam.of(Duration.ofSeconds(1800))))

            .schema(params.getOptional("schema", String.class))

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
}

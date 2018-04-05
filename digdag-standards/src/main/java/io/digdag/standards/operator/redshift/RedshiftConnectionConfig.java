package io.digdag.standards.operator.redshift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnectionConfig;
import io.digdag.util.DurationParam;
import org.immutables.value.Value;

import java.time.Duration;
import java.util.Properties;

@Value.Immutable
public abstract class RedshiftConnectionConfig
    extends AbstractJdbcConnectionConfig
{
    public abstract Optional<String> schema();

    @VisibleForTesting
    public static RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
    {
        return ImmutableRedshiftConnectionConfig.builder()
                .host(secrets.getSecretOptional("host").or(() -> params.get("host", String.class)))
                .port(secrets.getSecretOptional("port").transform(Integer::parseInt).or(() -> params.get("port", int.class, 5439)))
                .user(secrets.getSecretOptional("user").or(() -> params.get("user", String.class)))
                .password(secrets.getSecretOptional("password"))
                .database(secrets.getSecretOptional("database").or(() -> params.get("database", String.class)))
                .ssl(secrets.getSecretOptional("ssl").transform(Boolean::parseBoolean).or(() -> params.get("ssl", boolean.class, false)))
                .connectTimeout(DurationParam.of(Duration.ofSeconds(params.get("connect_timeout", int.class, 30))))
                .socketTimeout(DurationParam.of(Duration.ofSeconds(params.get("socket_timeout", int.class, 1800))))
                .schema(secrets.getSecretOptional("schema").or(params.getOptional("schema", String.class)))
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

package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.ExecutionException;

public class RedisServerClient
        implements ParamServerClient
{
    private RedisClient redisClient;

    public RedisServerClient(Config systemConfig)
    {
        String host = systemConfig.get("host", String.class, "localhost");
        Optional<Integer> port = systemConfig.getOptional("param_server.port", Integer.class);
        Optional<Boolean> ssl = systemConfig.getOptional("param_server.ssl", Boolean.class);
        Optional<String> password = systemConfig.getOptional("param_server.password", String.class);

        RedisURI.Builder redisUrlBuilder = RedisURI.Builder.redis(host);
        if (port.isPresent()) {
            redisUrlBuilder.withPort(port.get());
        }
        if (ssl.isPresent()) {
            redisUrlBuilder.withSsl(ssl.get());
        }
        if (password.isPresent()) {
            redisUrlBuilder.withPassword(password.get());
        }
        this.redisClient = RedisClient.create(redisUrlBuilder.build());
    }

    @Override
    public Optional<String> get(String key)
    {
        String result;
        try (StatefulRedisConnection<String, String> redisConnection = this.redisClient.connect()) {
            RedisCommands<String, String> commands = redisConnection.sync();
            result = commands.get(key);
        }

        return Optional.of(result);
    }

    @Override
    public void set(String key, String value)
    {
        try (StatefulRedisConnection<String, String> redisConnection = this.redisClient.connect()) {
            RedisCommands<String, String> commands = redisConnection.sync();
            commands.set(key, value);
        }
    }

    @Override
    public void finalize(){
        this.redisClient.shutdown();
    }
}

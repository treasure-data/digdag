package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import redis.clients.jedis.Jedis;

public class RedisServerClient
        implements ParamServerClient
{
    private Jedis redisClient;

    public RedisServerClient(Config systemConfig)
    {
        String host = systemConfig.get("param_server.host", String.class, "localhost");
        Integer port = systemConfig.get("param_server.port", Integer.class, 6379);
        Boolean ssl = systemConfig.get("param_server.ssl", Boolean.class, false);
        Optional<String> password = systemConfig.getOptional("param_server.password", String.class);

        this.redisClient = new Jedis(host, port, ssl);
        if (password.isPresent()) {
            redisClient.auth(password.get());
        }
        redisClient.connect();
    }

    @Override
    public Optional<String> get(String key, int siteId)
    {
        return Optional.of(redisClient.get(key));
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        redisClient.set(key, value);
    }

    @Override
    public void finalize()
    {
        redisClient.close();
    }
}

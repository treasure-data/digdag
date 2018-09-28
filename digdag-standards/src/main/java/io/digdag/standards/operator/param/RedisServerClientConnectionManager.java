package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.ParamServerClientConnectionManager;
import redis.clients.jedis.Jedis;

public class RedisServerClientConnectionManager
        implements ParamServerClientConnectionManager
{
    private final String host;
    private final Integer port;
    private final Boolean ssl;
    private final Optional<String> password;

    public RedisServerClientConnectionManager(Config systemConfig)
    {
        host = systemConfig.get("param_server.host", String.class, "localhost");
        port = systemConfig.get("param_server.port", Integer.class, 6379);
        ssl = systemConfig.get("param_server.ssl", Boolean.class, false);
        password = systemConfig.getOptional("param_server.password", String.class);
    }

    @Override
    public ParamServerClientConnection getConnection()
    {
        Jedis redisClient = new Jedis(host, port, ssl);
        if (password.isPresent()) {
            redisClient.auth(password.get());
        }

        redisClient.connect();
        return new RedisServerClientConnection(redisClient);
    }

    @Override
    public void shutdown()
    {
    }
}

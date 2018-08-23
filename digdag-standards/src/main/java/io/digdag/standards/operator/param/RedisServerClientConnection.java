package io.digdag.standards.operator.param;

import redis.clients.jedis.Jedis;

public class RedisServerClientConnection implements ParamServerClientConnection<Jedis>
{
    private final Jedis connection;

    public RedisServerClientConnection(Jedis connection){
        this.connection = connection;
    }

    @Override
    public Jedis get()
    {
        return this.connection;
    }

    @Override
    public String getType()
    {
        return "redis";
    }
}

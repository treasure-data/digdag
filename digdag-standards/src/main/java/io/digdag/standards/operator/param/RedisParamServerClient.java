package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/*
    This class is not Threadsafe, but this class is
    expected to be bound as NOT singleton in guice container.
    see ParamServerModule
 */
public class RedisParamServerClient
        implements ParamServerClient
{
    private Jedis connection;
    private Map<String, String> msetTarget = new HashMap<>();

    public RedisParamServerClient(ParamServerClientConnection connection)
    {
        this.connection = (Jedis) connection.get();
    }

    @Override
    public Optional<String> get(String key, int siteId)
    {
        if (connection == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        String result = connection.get(formattedKey(key, siteId));
        return result == null ? Optional.absent() : Optional.of(result);
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        if (connection == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        msetTarget.put(formattedKey(key, siteId), value);
    }

    @Override
    public void doTransaction(Consumer<ParamServerClient> consumer)
    {
        if (connection == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        consumer.accept(this);
        commit();
    }

    @Override
    public void commit()
    {
        if (connection != null) {
            if (!msetTarget.isEmpty()) {
                List<String> queries = new ArrayList<>();
                for (Map.Entry<String, String> entry : msetTarget.entrySet()) {
                    queries.add(entry.getKey());
                    queries.add(entry.getValue());
                }
                // redisClient.mset(key1, value1, key2, value2, ...)
                connection.mset(queries.toArray(new String[queries.size()]));
            }
            msetTarget.clear();

            // `connection` object is created for every time when ParamServerClientConnection
            // is injected, and is not a singleton object.
            // So we must close connection manually.
            connection.close();
            this.connection = null;
        }
    }

    private String formattedKey(String key, int siteId)
    {
        return String.valueOf(siteId) + ":" + key;
    }
}

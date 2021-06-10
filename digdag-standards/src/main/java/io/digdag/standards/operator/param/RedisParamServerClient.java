package io.digdag.standards.operator.param;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.Record;
import io.digdag.spi.ValueType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/*
    This class is not Threadsafe, but this class is
    expected to be bound as NOT singleton in guice container.
    see ParamGetOperatorFactory / ParamSetOperatorFactory
 */
public class RedisParamServerClient
        implements ParamServerClient
{
    private final ObjectMapper objectMapper;
    private Jedis connection;
    private Map<String, String> msetTarget = new HashMap<>();

    public RedisParamServerClient(ParamServerClientConnection connection, ObjectMapper objectMapper)
    {
        this.connection = (Jedis) connection.get();
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<Record> get(String key, int siteId)
    {
        if (connection == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        // "{value: {value: 1}, value_type: 0}"
        String rawResult = connection.get(formattedKey(key, siteId));
        if (rawResult == null) { return Optional.absent(); }

        JsonNode json;
        try {
            json = objectMapper.readTree(rawResult);
        }
        catch (IOException e) {
            throw ThrowablesUtil.propagate(e);
        }
        JsonNode value = json.get("value");
        int valueType = json.get("value_type").asInt();

        Record record = Record.builder()
                .key(key)
                .value(value)
                .valueType(ValueType.of(valueType))
                .build();

        return Optional.of(record);
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        if (connection == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        try {
            msetTarget.put(formattedKey(key, siteId), jsonizeBody(value));
        }
        catch (JsonProcessingException e) {
            throw ThrowablesUtil.propagate(e);
        }
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
                Transaction multi = connection.multi();
                for (Map.Entry<String, String> entry : msetTarget.entrySet()) {
                    multi.setex(entry.getKey(), DEFAULT_TTL_IN_SEC, entry.getValue());
                }
                multi.exec();
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

    private String jsonizeBody(String originalValue)
            throws JsonProcessingException
    {
        Map<String, String> value = new HashMap<String, String>()
        {{
            put("value", originalValue);
        }};

        return objectMapper.writeValueAsString(new HashMap<String, Object>()
        {{
            put("value", value);
            put("value_type", ValueType.STRING.ordinal());
        }});
    }
}

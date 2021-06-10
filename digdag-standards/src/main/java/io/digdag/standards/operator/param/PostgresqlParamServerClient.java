package io.digdag.standards.operator.param;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.Record;
import io.digdag.spi.ValueType;
import org.skife.jdbi.v2.Handle;

import java.util.HashMap;
import java.util.function.Consumer;

/*
    This class is not Threadsafe, but this class is
    expected to be bound as NOT singleton in guice container.
    see ParamGetOperatorFactory / ParamSetOperatorFactory
 */
public class PostgresqlParamServerClient
        implements ParamServerClient
{
    private final ObjectMapper objectMapper;
    private Handle handle;

    public PostgresqlParamServerClient(ParamServerClientConnection connection, ObjectMapper objectMapper)
    {
        this.handle = (Handle) connection.get();
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<Record> get(String key, int siteId)
    {
        if (handle == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        // In this method, timezone of `now()` is not specified.
        // Timezone depends on PostgreSQL server settings.
        Record rawRecord = handle
                .createQuery(
                        "select key, value, value_type" +
                                " from params" +
                                " where key = :key and site_id = :site_id" +
                                " and updated_at + INTERVAL '" + String.valueOf(DEFAULT_TTL_IN_SEC) + " seconds' >= now()" +
                                " limit 1")
                .bind("key", key)
                .bind("site_id", siteId)
                .mapTo(Record.class)
                .first();

        return rawRecord == null ? Optional.absent() : Optional.of(rawRecord);
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        if (handle == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        String jsonValue;
        try {
            jsonValue = jsonizeValue(value);
        }
        catch (JsonProcessingException e) {
            throw ThrowablesUtil.propagate(e);
        }

        handle.insert(
                "insert into params (key, value, value_type, site_id, created_at, updated_at) values (?, ?, ?, ?, now(), now()) " +
                        "on conflict on constraint params_site_id_key_uniq do update set value = ?, updated_at = now()",
                key, jsonValue, ValueType.STRING.ordinal(), siteId, jsonValue); // value_type is fixed to ValueType.STRING for now
    }

    @Override
    public void doTransaction(Consumer<ParamServerClient> consumer)
    {
        if (handle == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        consumer.accept(this);
        commit();
    }

    @Override
    public void commit()
    {
        if (handle != null) {
            // When the handle is created, handle.begin() is always called
            // at PostgresqlServerClientConnectionManager.getConnection().
            // So we must call commit() to close the transaction.
            handle.commit();

            // `handle` object is created for every time when ParamServerClientConnection
            // is injected, and is not a singleton object.
            // So we must close connection manually to return the connection to the connection pool.
            handle.close();
            this.handle = null;
        }
    }

    private String jsonizeValue(String originalValue)
            throws JsonProcessingException
    {
        return objectMapper.writeValueAsString(new HashMap<String, String>()
        {{
            put("value", originalValue);
        }});
    }
}

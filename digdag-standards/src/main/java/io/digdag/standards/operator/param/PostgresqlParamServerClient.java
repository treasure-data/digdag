package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.function.Consumer;

/*
    This class is not Threadsafe, but this class is
    expected to be bound as NOT singleton in guice container.
    see ParamServerModule
 */
public class PostgresqlParamServerClient
        implements ParamServerClient
{
    private Handle handle;

    @Inject
    public PostgresqlParamServerClient(ParamServerClientConnection connection)
    {
        this.handle = (Handle) connection.get();
    }

    @Override
    public Optional<String> get(String key, int siteId)
    {
        if (handle == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        List<String> record = handle
                .createQuery("select value from params where key = :key and site_id = :site_id limit 1")
                .bind("key", key)
                .bind("site_id", siteId)
                .mapTo(String.class)
                .list();

        return record.size() > 0 ? Optional.of(record.get(0)) : Optional.absent();
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        if (handle == null) {
            throw new IllegalStateException("Connection has already closed");
        }

        handle.insert(
                "insert into params (key, value, site_id, created_at, updated_at) values (?, ?, ?, now(), now()) " +
                        "on conflict on constraint params_site_id_key_uniq do update set value = ?, updated_at = now()",
                key, value, siteId, value);
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
}

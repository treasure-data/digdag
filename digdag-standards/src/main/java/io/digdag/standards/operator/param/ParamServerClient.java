package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import io.digdag.spi.Record;

import java.util.function.Consumer;

public interface ParamServerClient
{
    // default ttl for each record is 90 days
    int DEFAULT_TTL = 60 * 24 * 90;

    Optional<Record> get(String key, int sitedId);

    void set(String key, String value, int siteId);

    void doTransaction(Consumer<ParamServerClient> consumer);

    void commit();
}

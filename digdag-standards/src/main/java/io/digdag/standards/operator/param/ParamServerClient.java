package io.digdag.standards.operator.param;

import com.google.common.base.Optional;

public interface ParamServerClient
{
    Optional<String> get(String key, int sitedId);

    void set(String key, String value, int siteId);

    void finalize();
}

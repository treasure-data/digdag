package io.digdag.spi;

import com.google.common.base.Optional;

public interface AccountRouting
{
    enum ModuleType
    {
        EXECUTOR("executor"),
        AGENT("agent"),
        SCHEDULER("scheduler");

        private final String module;

        ModuleType(String module)
        {
            this.module = module;
        }

        @Override
        public String toString()
        {
            return module;
        }
    }

    String DEFAULT_COLUMN = "site_id";

    Boolean enabled();

    String getFilterSQL(String column);

    default String getFilterSQL()
    {
        return getFilterSQL(DEFAULT_COLUMN);
    }

    default Optional<String> getFilterSQLOpt(String column)
    {
        return enabled() ? Optional.of(getFilterSQL()) : Optional.absent();
    }

    default Optional<String> getFilterSQLOpt()
    {
        return getFilterSQLOpt(DEFAULT_COLUMN);
    }
}

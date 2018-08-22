package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.digdag.core.database.DataSourceProvider;
import io.digdag.core.database.DatabaseConfig;

public class ParamServerDataSourceProvider
        extends DataSourceProvider
{
    @Inject
    public ParamServerDataSourceProvider(@Named("param_server.database") DatabaseConfig config)
    {
        super(config);
    }
}

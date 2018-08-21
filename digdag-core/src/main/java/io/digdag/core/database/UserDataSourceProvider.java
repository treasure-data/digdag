package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class UserDataSourceProvider
        extends DataSourceProvider
{
    @Inject
    public UserDataSourceProvider(@Named("user_database") DatabaseConfig config)
    {
        super(config);
    }
}

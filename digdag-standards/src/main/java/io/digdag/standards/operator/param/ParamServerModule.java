package io.digdag.standards.operator.param;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.digdag.core.database.DatabaseConfig;
import org.skife.jdbi.v2.DBI;

import javax.sql.DataSource;

public class ParamServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DatabaseConfig.class)
                .annotatedWith(Names.named("param_server.database"))
                .toProvider(ParamServerDatabaseConfigProvider.class).in(Scopes.SINGLETON);
        binder.bind(DataSource.class)
                .annotatedWith(Names.named("param_server.database"))
                .toProvider(ParamServerDataSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DBI.class)
                .annotatedWith(Names.named("param_server.database"))
                .toProvider(ParamServerDbiProvider.class);  // don't make this singleton because DBI.registerMapper is called for each StoreManager
    }
}

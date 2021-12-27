package io.digdag.standards.operator.param;

import io.digdag.spi.ParamServerClientConnection;
import org.jdbi.v3.core.Handle;

public class PostgresqlServerClientConnection implements ParamServerClientConnection<Handle>
{
    private final Handle handle;

    public PostgresqlServerClientConnection(Handle handle){
        this.handle = handle;
    }

    @Override
    public Handle get()
    {
        return this.handle;
    }

    @Override
    public String getType()
    {
        return "postgresql";
    }
}

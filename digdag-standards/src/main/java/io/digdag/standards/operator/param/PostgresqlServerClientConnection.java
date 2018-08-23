package io.digdag.standards.operator.param;

import org.skife.jdbi.v2.Handle;

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

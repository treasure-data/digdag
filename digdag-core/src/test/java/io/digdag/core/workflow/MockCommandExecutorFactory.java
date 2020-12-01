package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;

public class MockCommandExecutorFactory
        implements CommandExecutorFactory
{
    @Inject
    public MockCommandExecutorFactory()
    { }

    @Override
    public String getType()
    {
        return "mock";
    }

    @Override
    public CommandExecutor newCommandExecutor()
    {
        return new MockCommandExecutor();
    }
}

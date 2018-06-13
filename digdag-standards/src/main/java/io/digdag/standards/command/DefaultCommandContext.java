package io.digdag.standards.command;

import io.digdag.client.config.Config;
import io.digdag.spi.CommandContext;
import io.digdag.spi.OperatorContext;
import java.nio.file.Path;
import java.util.List;

public class DefaultCommandContext
        implements CommandContext
{
    private final Config params;
    private final String script;
    private final List<String> cmdline;
    private final Path workspacePath;
    private final OperatorContext operatorContext;

    public DefaultCommandContext(Config params, String script, List<String> cmdline,
            OperatorContext operatorContext, Path workspacePath)
    {
        this.params = params;
        this.script = script;
        this.cmdline = cmdline;
        this.operatorContext = operatorContext;
        this.workspacePath = workspacePath;
    }

    @Override
    public Config getParams()
    {
        return params;
    }

    @Override
    public String getScript()
    {
        return script;
    }

    @Override
    public List<String> getCommandLine()
    {
        return cmdline;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public Path getWorkspacePath()
    {
        return workspacePath;
    }
}

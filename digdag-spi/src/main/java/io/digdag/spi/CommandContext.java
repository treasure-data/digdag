package io.digdag.spi;

import io.digdag.client.config.Config;
import java.nio.file.Path;
import java.util.List;

public interface CommandContext
{
    Config getParams();

    String getScript();

    List<String> getCommandLine();

    OperatorContext getOperatorContext();

    Path getWorkspacePath();
}
